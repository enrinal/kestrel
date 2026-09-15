import Foundation

/// Which records to export.
public struct ExportRequest: Sendable {
    public let topic: String
    /// Partitions to read, in order. Empty means every partition.
    public let partitions: [Int32]
    /// First offset to include. `nil` starts at the oldest retained record.
    public let startOffset: Int64?
    /// One past the last offset to include, matching how Kafka reports a
    /// partition's high watermark. `nil` runs to the end of the partition as it
    /// stands when the export begins.
    public let endOffset: Int64?

    public init(
        topic: String,
        partitions: [Int32] = [],
        startOffset: Int64? = nil,
        endOffset: Int64? = nil
    ) {
        self.topic = topic
        self.partitions = partitions
        self.startOffset = startOffset
        self.endOffset = endOffset
    }
}

/// What an export produced.
public struct ExportOutcome: Sendable, Equatable {
    public let records: Int
    /// Records taken from each partition, for the summary line.
    public let byPartition: [Int32: Int]
    /// Bytes written, when the destination was a file.
    public let bytesWritten: Int

    public init(records: Int, byPartition: [Int32: Int], bytesWritten: Int = 0) {
        self.records = records
        self.byPartition = byPartition
        self.bytesWritten = bytesWritten
    }
}

/// Where exported records go.
///
/// Sinks are actors: an export reads from the broker off the main thread, and
/// writing must not hop back onto it for every record. Being an actor is also
/// what makes a sink safe to hand to the exporter in the first place.
public protocol ExportSink: Sendable {
    /// Writes one record.
    ///
    /// - Parameters:
    ///   - record: the record, with its original partition and offset.
    ///   - topic: the topic it came from, recorded in the envelope.
    func write(_ record: KafkaRecord, from topic: String) async throws

    /// Releases anything held open. Called once, even if the export failed.
    func finish() async throws

    /// Bytes written, for sinks that write bytes. Zero for the others.
    var bytesWritten: Int { get async }
}

/// Writes records as JSON Lines: one ``RecordEnvelope`` per line.
///
/// This is the format ``RecordImporter`` reads, so an export can be imported
/// straight back. Records are appended as they arrive rather than gathered up
/// first, since an export can be far larger than memory.
public actor JSONLFileSink: ExportSink {
    private let handle: FileHandle
    private let encoder: JSONEncoder
    private var written = 0

    public var bytesWritten: Int { written }

    /// Creates the file, replacing anything already there.
    public init(url: URL) throws {
        FileManager.default.createFile(atPath: url.path, contents: nil)
        guard let handle = FileHandle(forWritingAtPath: url.path) else {
            throw CocoaError(.fileWriteUnknown)
        }
        // Truncated explicitly: an existing longer file would otherwise keep
        // its tail past the end of what this export writes.
        try handle.truncate(atOffset: 0)
        self.handle = handle

        // Compact and sorted: one line per record, and stable enough to diff.
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        self.encoder = encoder
    }

    public func write(_ record: KafkaRecord, from topic: String) async throws {
        var line = try encoder.encode(RecordEnvelope(record: record, topic: topic))
        line.append(0x0a)
        try handle.write(contentsOf: line)
        written += line.count
    }

    public func finish() async throws {
        try handle.close()
    }
}

/// Produces exported records to another topic.
public actor TopicSink: ExportSink {
    private let produce: @Sendable (KafkaRecord) async throws -> DeliveryReport
    /// Offsets assigned in the destination topic, in the order written.
    public private(set) var reports: [DeliveryReport] = []

    public var bytesWritten: Int { 0 }

    /// - Parameter produce: sends one record to the destination topic.
    public init(produce: @escaping @Sendable (KafkaRecord) async throws -> DeliveryReport) {
        self.produce = produce
    }

    public func write(_ record: KafkaRecord, from topic: String) async throws {
        reports.append(try await produce(record))
    }

    public func finish() async throws {}
}

/// Reads a range of records out of a topic and hands them to a sink.
public enum RecordExporter {
    /// How many records to read from the broker at a time.
    ///
    /// Large enough to keep the round trips down, small enough that progress
    /// moves visibly and one page never dominates memory.
    public static let pageSize = 500

    /// Counts the records a request covers, so progress has a total.
    ///
    /// The count is what the partitions hold when asked; records produced or
    /// deleted afterwards are not reflected. It is an estimate for a progress
    /// bar, not a guarantee.
    ///
    /// - Returns: the per-partition ranges to read, and their total.
    public static func plan(
        request: ExportRequest,
        consumer: KafkaConsumer
    ) async throws -> (ranges: [(partition: Int32, start: Int64, end: Int64)], total: Int) {
        var ranges: [(partition: Int32, start: Int64, end: Int64)] = []

        for partition in request.partitions {
            let marks = try await consumer.watermarks(topic: request.topic, partition: partition)
            // Clamped to what is retained: a start before the low watermark
            // would otherwise report records that no longer exist.
            let start = max(request.startOffset ?? marks.low, marks.low)
            let end = min(request.endOffset ?? marks.high, marks.high)
            if end > start {
                ranges.append((partition, start, end))
            }
        }

        return (ranges, ranges.reduce(0) { $0 + Int($1.end - $1.start) })
    }

    /// Exports every record in the request.
    ///
    /// Partitions are read in order, each from its start offset up to but not
    /// including its end offset.
    ///
    /// - Parameters:
    ///   - request: what to export. Its partitions must be listed; use
    ///     ``ExportRequest`` with the topic's partition ids.
    ///   - consumer: reader for the source topic.
    ///   - sink: where records go. ``finish()`` is called before returning,
    ///     including when the export throws.
    ///   - isolation: inherited from the caller, so a view can pass a progress
    ///     closure that touches its own state.
    ///   - progress: called with records written so far and the estimated total.
    /// - Returns: how many records were written, and from where.
    public static func export(
        request: ExportRequest,
        consumer: KafkaConsumer,
        sink: ExportSink,
        isolation: isolated (any Actor)? = #isolation,
        progress: ((Int, Int) -> Void)? = nil
    ) async throws -> ExportOutcome {
        let (ranges, total) = try await plan(request: request, consumer: consumer)

        var byPartition: [Int32: Int] = [:]
        var written = 0

        do {
            for range in ranges {
                var offset = range.start

                while offset < range.end {
                    let wanted = Int(min(Int64(pageSize), range.end - offset))
                    let page = try await consumer.fetch(
                        topic: request.topic,
                        partition: range.partition,
                        from: .offset(offset),
                        limit: wanted
                    )

                    // An empty page means the range is no longer readable —
                    // deleted by retention, say — and looping again would spin
                    // forever.
                    if page.records.isEmpty { break }

                    for record in page.records where record.offset < range.end {
                        try await sink.write(record, from: request.topic)
                        written += 1
                        byPartition[range.partition, default: 0] += 1
                    }

                    progress?(written, total)
                    offset = (page.records.last?.offset ?? offset) + 1
                }
            }
        } catch {
            try? await sink.finish()
            throw error
        }

        let bytes = await sink.bytesWritten
        try await sink.finish()

        return ExportOutcome(
            records: written,
            byPartition: byPartition,
            bytesWritten: bytes
        )
    }
}
