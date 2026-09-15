import Foundation

/// A record header. Kafka header values are arbitrary bytes, not text.
public struct RecordHeader: Identifiable, Hashable, Sendable {
    public let name: String
    public let value: Data?

    public var id: String { name }

    /// Value decoded as UTF-8, or a byte-count placeholder when it is not text.
    public var displayValue: String {
        guard let value else { return "—" }
        if let text = String(data: value, encoding: .utf8) { return text }
        return "\(value.count) bytes"
    }

    public init(name: String, value: Data?) {
        self.name = name
        self.value = value
    }
}

/// How a record's timestamp was set.
public enum RecordTimestampKind: Sendable {
    case unavailable
    case createTime
    case logAppendTime
}

/// One record read from a partition.
public struct KafkaRecord: Identifiable, Sendable {
    public let partition: Int32
    public let offset: Int64
    public let timestamp: Date?
    public let timestampKind: RecordTimestampKind
    public let key: Data?
    public let value: Data?
    public let headers: [RecordHeader]

    /// Unique within a topic, which is what the record table needs.
    public var id: String { "\(partition):\(offset)" }

    public var valueByteCount: Int { value?.count ?? 0 }
    public var isTombstone: Bool { value == nil }

    public init(
        partition: Int32,
        offset: Int64,
        timestamp: Date?,
        timestampKind: RecordTimestampKind,
        key: Data?,
        value: Data?,
        headers: [RecordHeader]
    ) {
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp
        self.timestampKind = timestampKind
        self.key = key
        self.value = value
        self.headers = headers
    }

    /// Key as text, or a placeholder for absent and binary keys.
    public var keyPreview: String {
        Self.preview(of: key, absent: "null")
    }

    /// Single-line value preview for the record table.
    ///
    /// Newlines and tabs are collapsed so one record occupies one row.
    public var valuePreview: String {
        guard value != nil else { return "null (tombstone)" }
        return Self.preview(of: value, absent: "null")
    }

    private static func preview(of data: Data?, absent: String, limit: Int = 200) -> String {
        guard let data else { return absent }
        if data.isEmpty { return "empty" }
        guard let text = String(data: data.prefix(limit * 4), encoding: .utf8) else {
            return "<\(data.count) bytes binary>"
        }
        let flattened = text
            .replacingOccurrences(of: "\n", with: " ")
            .replacingOccurrences(of: "\r", with: " ")
            .replacingOccurrences(of: "\t", with: " ")
        return flattened.count > limit ? String(flattened.prefix(limit)) + "…" : flattened
    }
}

/// Where a read should start.
public enum StartPosition: Sendable, Equatable {
    /// The oldest record still retained.
    case earliest
    /// The last `count` records in the partition.
    case latest(count: Int64)
    /// A specific offset, clamped into the retained range.
    case offset(Int64)
}

/// One page of records, plus the partition bounds it was read against.
public struct RecordPage: Sendable {
    public let records: [KafkaRecord]
    /// Offset the read actually started from, after resolving ``StartPosition``.
    public let startOffset: Int64
    /// Oldest retained offset.
    public let lowWatermark: Int64
    /// Offset the next produced record will get.
    public let highWatermark: Int64
    /// True when the read reached the end of the partition.
    public let reachedEnd: Bool

    /// True when the partition holds no records at all.
    public var isPartitionEmpty: Bool { lowWatermark >= highWatermark }

    public init(
        records: [KafkaRecord],
        startOffset: Int64,
        lowWatermark: Int64,
        highWatermark: Int64,
        reachedEnd: Bool
    ) {
        self.records = records
        self.startOffset = startOffset
        self.lowWatermark = lowWatermark
        self.highWatermark = highWatermark
        self.reachedEnd = reachedEnd
    }
}

/// Where the broker put a produced record.
public struct DeliveryReport: Sendable, Equatable {
    public let partition: Int32
    public let offset: Int64

    public init(partition: Int32, offset: Int64) {
        self.partition = partition
        self.offset = offset
    }
}
