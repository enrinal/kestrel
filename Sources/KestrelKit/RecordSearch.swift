import Foundation

/// What to look for.
public struct SearchQuery: Sendable {
    public let text: String
    /// Treat ``text`` as a regular expression rather than a literal.
    public let isRegex: Bool
    public let searchesKeys: Bool
    public let searchesValues: Bool
    public let isCaseSensitive: Bool
    /// Stop after this many hits, so a broad query cannot run forever.
    public let maxHits: Int

    public init(
        text: String,
        isRegex: Bool = false,
        searchesKeys: Bool = true,
        searchesValues: Bool = true,
        isCaseSensitive: Bool = false,
        maxHits: Int = 500
    ) {
        self.text = text
        self.isRegex = isRegex
        self.searchesKeys = searchesKeys
        self.searchesValues = searchesValues
        self.isCaseSensitive = isCaseSensitive
        self.maxHits = maxHits
    }
}

/// Where a match was found.
public enum SearchField: String, Sendable {
    case key
    case value
}

/// One matching record.
public struct SearchHit: Sendable, Identifiable, Equatable {
    public let topic: String
    public let partition: Int32
    public let offset: Int64
    public let field: SearchField
    /// A short excerpt around the match, for the results list.
    public let excerpt: String

    public var id: String { "\(topic):\(partition):\(offset):\(field.rawValue)" }

    public init(
        topic: String,
        partition: Int32,
        offset: Int64,
        field: SearchField,
        excerpt: String
    ) {
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.field = field
        self.excerpt = excerpt
    }
}

/// Which partitions a scan covers.
public struct SearchScope: Sendable {
    public let topic: String
    public let partitions: [Int32]

    public init(topic: String, partitions: [Int32]) {
        self.topic = topic
        self.partitions = partitions
    }
}

/// How a scan ended.
public struct SearchOutcome: Sendable {
    public let hits: [SearchHit]
    /// Records examined, which is what the progress line counts.
    public let scanned: Int
    /// True when the scan stopped at ``SearchQuery/maxHits`` with more to come.
    public let reachedLimit: Bool
    /// True when the scan stopped early because it was cancelled.
    ///
    /// A scan that ran to completion reports `false` even if its task was
    /// cancelled afterwards: the results are whole either way, and saying
    /// otherwise would have the UI call a finished search incomplete.
    public let wasCancelled: Bool

    public init(hits: [SearchHit], scanned: Int, reachedLimit: Bool, wasCancelled: Bool) {
        self.hits = hits
        self.scanned = scanned
        self.reachedLimit = reachedLimit
        self.wasCancelled = wasCancelled
    }
}

/// Reads records and reports the ones that match.
///
/// An actor: a scan reads whole partitions and matches every record, which must
/// not happen where the window draws. Cancellation is cooperative — the scan
/// checks for it between pages and between records, and returns what it found
/// rather than throwing, since partial results are still useful.
public actor RecordSearcher {
    /// Records read from the broker at a time.
    public static let pageSize = 500

    private let consumer: KafkaConsumer

    public init(consumer: KafkaConsumer) {
        self.consumer = consumer
    }

    /// Scans every partition in scope.
    ///
    /// - Parameters:
    ///   - query: what to look for.
    ///   - scopes: topics and partitions to read.
    ///   - progress: called with records scanned and hits found so far, from
    ///     outside the caller's isolation.
    /// - Returns: the hits, in the order the records were read.
    /// - Throws: ``SearchError/invalidPattern`` if a regex query will not
    ///   compile. Broker failures on one partition do not throw: they end that
    ///   partition and the scan moves on.
    public func scan(
        query: SearchQuery,
        scopes: [SearchScope],
        progress: (@Sendable (Int, Int) -> Void)? = nil
    ) async throws -> SearchOutcome {
        let matcher = try Matcher(query: query)

        var hits: [SearchHit] = []
        var scanned = 0
        var reachedLimit = false
        var stoppedEarly = false

        for scope in scopes {
            for partition in scope.partitions {
                if Task.isCancelled { stoppedEarly = true }
                if stoppedEarly || reachedLimit { break }

                // A partition whose watermarks cannot be read is skipped
                // rather than failing the whole scan: one unavailable leader
                // should not lose the hits already found elsewhere.
                guard let marks = try? await consumer.watermarks(
                    topic: scope.topic,
                    partition: partition
                ) else { continue }

                var offset = marks.low
                while offset < marks.high {
                    if Task.isCancelled { stoppedEarly = true }
                    if stoppedEarly || reachedLimit { break }

                    let wanted = Int(min(Int64(Self.pageSize), marks.high - offset))
                    guard let page = try? await consumer.fetch(
                        topic: scope.topic,
                        partition: partition,
                        from: .offset(offset),
                        limit: wanted
                    ), !page.records.isEmpty else { break }

                    for record in page.records {
                        // Checked per record as well as per page: a page is 500
                        // records, and a cancelled search should not have to
                        // finish matching all of them.
                        if Task.isCancelled {
                            stoppedEarly = true
                            break
                        }
                        scanned += 1

                        if query.searchesKeys,
                           let excerpt = matcher.match(record.key) {
                            hits.append(
                                SearchHit(
                                    topic: scope.topic,
                                    partition: partition,
                                    offset: record.offset,
                                    field: .key,
                                    excerpt: excerpt
                                )
                            )
                        }

                        if query.searchesValues,
                           let excerpt = matcher.match(record.value) {
                            hits.append(
                                SearchHit(
                                    topic: scope.topic,
                                    partition: partition,
                                    offset: record.offset,
                                    field: .value,
                                    excerpt: excerpt
                                )
                            )
                        }

                        if hits.count >= query.maxHits {
                            reachedLimit = true
                            break
                        }
                    }

                    progress?(scanned, hits.count)
                    offset = (page.records.last?.offset ?? offset) + 1
                }
            }
        }

        return SearchOutcome(
            hits: hits,
            scanned: scanned,
            reachedLimit: reachedLimit,
            wasCancelled: stoppedEarly
        )
    }
}

/// Why a query could not be run.
public enum SearchError: LocalizedError, Equatable {
    case invalidPattern(String)
    case nothingToSearch

    public var errorDescription: String? {
        switch self {
        case .invalidPattern(let detail):
            "Not a valid regular expression: \(detail)"
        case .nothingToSearch:
            "Search the key, the value, or both."
        }
    }
}

/// Decides whether one payload matches, and quotes the part that did.
///
/// Public so the matching rules — what counts as text, how an excerpt is
/// trimmed — can be checked without reading a topic.
public struct Matcher {
    private let query: SearchQuery
    private let regex: NSRegularExpression?

    public init(query: SearchQuery) throws {
        guard query.searchesKeys || query.searchesValues else {
            throw SearchError.nothingToSearch
        }
        self.query = query

        if query.isRegex {
            do {
                regex = try NSRegularExpression(
                    pattern: query.text,
                    options: query.isCaseSensitive ? [] : [.caseInsensitive]
                )
            } catch {
                throw SearchError.invalidPattern(error.localizedDescription)
            }
        } else {
            regex = nil
        }
    }

    /// - Parameter payload: bytes to test. `nil` and non-text never match.
    /// - Returns: an excerpt around the match, or `nil` if it does not match.
    public func match(_ payload: Data?) -> String? {
        // Binary payloads are not searched: decoding them lossily would invent
        // matches that are not in the data.
        guard let payload, let text = String(data: payload, encoding: .utf8) else { return nil }

        if let regex {
            let range = NSRange(text.startIndex..., in: text)
            guard let found = regex.firstMatch(in: text, range: range),
                  let matched = Range(found.range, in: text)
            else { return nil }
            return excerpt(of: text, around: matched)
        }

        guard let found = text.range(
            of: query.text,
            options: query.isCaseSensitive ? [] : [.caseInsensitive]
        ) else { return nil }
        return excerpt(of: text, around: found)
    }

    /// Up to 40 characters either side of the match, with ellipses when cut.
    private func excerpt(of text: String, around range: Range<String.Index>) -> String {
        let padding = 40
        let start = text.index(range.lowerBound, offsetBy: -padding, limitedBy: text.startIndex)
            ?? text.startIndex
        let end = text.index(range.upperBound, offsetBy: padding, limitedBy: text.endIndex)
            ?? text.endIndex

        var excerpt = String(text[start..<end]).replacingOccurrences(of: "\n", with: " ")
        if start != text.startIndex { excerpt = "…" + excerpt }
        if end != text.endIndex { excerpt += "…" }
        return excerpt
    }
}
