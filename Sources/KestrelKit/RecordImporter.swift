import Foundation

/// One record read from an import file, ready to produce.
public struct ImportRecord: Sendable, Equatable {
    /// 1-based line in the source file, for reporting.
    public let line: Int
    public let key: Data?
    public let value: Data?
    public let headers: [RecordHeader]
    /// Only set when the file says so; otherwise the broker partitions.
    public let partition: Int32?

    public init(
        line: Int,
        key: Data? = nil,
        value: Data?,
        headers: [RecordHeader] = [],
        partition: Int32? = nil
    ) {
        self.line = line
        self.key = key
        self.value = value
        self.headers = headers
        self.partition = partition
    }
}

/// A line that could not be read.
public struct ImportProblem: Sendable, Equatable, Identifiable {
    public let line: Int
    public let message: String

    public var id: Int { line }

    public init(line: Int, message: String) {
        self.line = line
        self.message = message
    }
}

/// How an import file turned out to be laid out.
public enum ImportSourceKind: String, Sendable {
    /// One ``RecordEnvelope`` per line, or a single envelope in the whole file.
    case envelopes
    /// One JSON document per line, each taken as a record value.
    case jsonLines
    /// Envelopes and plain documents in the same file.
    case mixed
}

/// What an import file contains, before anything is produced.
public struct ImportPlan: Sendable {
    public let kind: ImportSourceKind
    public let records: [ImportRecord]
    /// Lines that could not be read. Importing skips them and reports them.
    public let problems: [ImportProblem]
    /// Blank lines, which are skipped without complaint.
    public let blankLines: Int

    public init(
        kind: ImportSourceKind,
        records: [ImportRecord],
        problems: [ImportProblem],
        blankLines: Int
    ) {
        self.kind = kind
        self.records = records
        self.problems = problems
        self.blankLines = blankLines
    }

    public var isEmpty: Bool { records.isEmpty }
}

/// Reads records out of a file so they can be produced to a topic.
///
/// Two shapes are understood, and mixing them in one file is allowed:
///
/// - a ``RecordEnvelope``, as written when saving a record, which carries the
///   key, headers and original value bytes;
/// - any other JSON document, taken whole as the record's value.
///
/// A file holding a single JSON document is one record; otherwise each line is
/// one record.
public enum RecordImporter {
    /// Reads and plans an import without producing anything.
    public static func plan(fileURL: URL) throws -> ImportPlan {
        plan(data: try Data(contentsOf: fileURL))
    }

    /// Reads and plans an import from bytes already in memory.
    ///
    /// Never throws: a file this cannot read yields problems rather than an
    /// error, so the sheet can show which lines are at fault.
    public static func plan(data: Data) -> ImportPlan {
        // A single document, possibly pretty-printed across many lines, is one
        // record. A JSON Lines file fails this parse on the second document,
        // so it falls through to line-by-line reading.
        if let whole = try? JSONSerialization.jsonObject(with: data), whole is [String: Any] {
            switch read(line: 1, bytes: data) {
            case .success(let record, let kind):
                return ImportPlan(kind: kind, records: [record], problems: [], blankLines: 0)
            case .failure(let problem):
                return ImportPlan(kind: .jsonLines, records: [], problems: [problem], blankLines: 0)
            }
        }

        var records: [ImportRecord] = []
        var problems: [ImportProblem] = []
        var blankLines = 0
        var sawEnvelope = false
        var sawDocument = false

        // Split on bytes rather than characters so a line with invalid UTF-8
        // is reported as one bad line instead of derailing the whole file.
        var lines = data.split(separator: UInt8(0x0a), omittingEmptySubsequences: false)
        // A file ending in a newline has as many lines as it has documents;
        // the empty remainder after the last newline is not a line of its own.
        if lines.last?.isEmpty == true { lines.removeLast() }

        for (index, line) in lines.enumerated() {
            let number = index + 1
            let bytes = Data(line)

            if bytes.allSatisfy({ $0 == 0x20 || $0 == 0x09 || $0 == 0x0d || $0 == 0x00 }) {
                blankLines += 1
                continue
            }

            switch read(line: number, bytes: bytes) {
            case .success(let record, let kind):
                records.append(record)
                if kind == .envelopes { sawEnvelope = true } else { sawDocument = true }
            case .failure(let problem):
                problems.append(problem)
            }
        }

        let kind: ImportSourceKind =
            switch (sawEnvelope, sawDocument) {
            case (true, true): .mixed
            case (true, false): .envelopes
            default: .jsonLines
            }

        return ImportPlan(
            kind: kind,
            records: records,
            problems: problems,
            blankLines: blankLines
        )
    }

    private enum LineResult {
        case success(ImportRecord, ImportSourceKind)
        case failure(ImportProblem)
    }

    /// Reads one line, as an envelope if it is marked as one.
    private static func read(line: Int, bytes: Data) -> LineResult {
        do {
            _ = try JSONSerialization.jsonObject(with: bytes)
        } catch {
            return .failure(
                ImportProblem(
                    line: line,
                    message: "Not valid JSON: \(PayloadFormatter.reason(from: error))"
                )
            )
        }

        // The marker is what separates a saved envelope from a document that
        // merely happens to have a `value` field. Without it, a payload such as
        // {"value": 1} would be mistaken for an envelope and produce the wrong
        // bytes.
        if isEnvelope(bytes) {
            do {
                let envelope = try JSONDecoder().decode(RecordEnvelope.self, from: bytes)
                guard envelope.value == nil || envelope.valueBytes != nil else {
                    return .failure(
                        ImportProblem(line: line, message: "Envelope value is not valid for its encoding")
                    )
                }
                guard envelope.key == nil || envelope.keyBytes != nil else {
                    return .failure(
                        ImportProblem(line: line, message: "Envelope key is not valid for its encoding")
                    )
                }
                return .success(
                    ImportRecord(
                        line: line,
                        key: envelope.keyBytes,
                        value: envelope.valueBytes,
                        headers: envelope.recordHeaders
                    ),
                    .envelopes
                )
            } catch {
                return .failure(
                    ImportProblem(line: line, message: "Not a usable envelope: \(error.localizedDescription)")
                )
            }
        }

        // Any other document is the value, kept byte for byte rather than
        // re-encoded, so what lands in Kafka is what was in the file.
        return .success(ImportRecord(line: line, value: bytes), .jsonLines)
    }

    /// Whether a document carries the envelope marker.
    private static func isEnvelope(_ bytes: Data) -> Bool {
        struct Probe: Decodable { let envelopeVersion: Int? }
        return (try? JSONDecoder().decode(Probe.self, from: bytes))?.envelopeVersion != nil
    }

    /// Produces every record in a plan, carrying on past failures.
    ///
    /// One bad record does not abandon the rest: a file of a thousand lines
    /// with one rejection should still import the other 999, and the caller
    /// reports what failed.
    ///
    /// - Parameters:
    ///   - plan: what to produce, from ``plan(fileURL:)``. Problems found while
    ///     reading are carried into the outcome.
    ///   - progress: called after each record with the number attempted and the
    ///     total, for a progress bar.
    ///   - produce: sends one record and returns where it landed.
    ///   - isolation: inherited from the caller, so a view can pass closures
    ///     that touch its own state without them having to be `Sendable`.
    /// - Returns: how many records the broker accepted, and every problem.
    public static func run(
        plan: ImportPlan,
        isolation: isolated (any Actor)? = #isolation,
        progress: ((Int, Int) -> Void)? = nil,
        produce: (ImportRecord) async throws -> DeliveryReport
    ) async -> ImportOutcome {
        var refused: [ImportProblem] = []
        var reports: [DeliveryReport] = []
        let total = plan.records.count

        for (index, record) in plan.records.enumerated() {
            do {
                reports.append(try await produce(record))
            } catch {
                refused.append(
                    ImportProblem(
                        line: record.line,
                        message: (error as? LocalizedError)?.errorDescription
                            ?? error.localizedDescription
                    )
                )
            }
            progress?(index + 1, total)
        }

        return ImportOutcome(
            produced: reports.count,
            problems: (plan.problems + refused).sorted { $0.line < $1.line },
            produceProblems: refused,
            reports: reports
        )
    }
}

/// How an import finished.
public struct ImportOutcome: Sendable {
    /// Records the broker accepted.
    public let produced: Int
    /// Everything that went wrong: lines skipped while reading, plus records
    /// the broker refused.
    public let problems: [ImportProblem]
    /// Only the records the broker refused.
    ///
    /// Kept apart from ``problems`` because a caller that already showed the
    /// unreadable lines when the file was read would otherwise list them a
    /// second time, under a heading that says they failed to send.
    public let produceProblems: [ImportProblem]
    /// Offsets assigned, in the order produced.
    public let reports: [DeliveryReport]

    public init(
        produced: Int,
        problems: [ImportProblem],
        produceProblems: [ImportProblem] = [],
        reports: [DeliveryReport]
    ) {
        self.produced = produced
        self.problems = problems
        self.produceProblems = produceProblems
        self.reports = reports
    }
}
