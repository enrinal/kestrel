import Foundation

/// One partition's committed position for a consumer group.
public struct GroupPartitionOffset: Identifiable, Sendable {
    public let topic: String
    public let partition: Int32
    /// Committed offset, or `nil` when the group has never committed here.
    public let committed: Int64?
    /// Offset the next produced record will get, i.e. the log end.
    public let logEnd: Int64
    /// Error reported for this partition, if any.
    public let error: String?

    public var id: String { "\(topic):\(partition)" }

    /// Records behind the log end, or `nil` without a committed offset.
    ///
    /// Clamped at zero: a committed offset can briefly exceed the watermark
    /// this call observed, and a negative lag would only confuse.
    public var lag: Int64? {
        guard let committed else { return nil }
        return max(0, logEnd - committed)
    }

    public init(
        topic: String,
        partition: Int32,
        committed: Int64?,
        logEnd: Int64,
        error: String? = nil
    ) {
        self.topic = topic
        self.partition = partition
        self.committed = committed
        self.logEnd = logEnd
        self.error = error
    }
}

/// Where a group's offsets should be moved to.
public enum OffsetReset: Sendable, Equatable {
    case earliest
    case latest
    case offset(Int64)
}

/// The partitions one group member is assigned.
public struct MemberAssignment: Identifiable, Hashable, Sendable {
    public let topic: String
    public let partitions: [Int32]

    public var id: String { topic }

    public init(topic: String, partitions: [Int32]) {
        self.topic = topic
        self.partitions = partitions
    }
}

/// Decodes the `ConsumerProtocolAssignment` blob librdkafka reports per member.
///
/// The consumer protocol is not part of librdkafka's API, so the bytes are
/// decoded here. Layout, all big-endian:
///
///     int16 version
///     int32 topic count
///       int16 name length, name bytes, int32 partition count, int32 partitions…
///     int32 user data length, user data bytes
///
/// A member that has not been assigned anything yet carries an empty blob.
public enum AssignmentDecoder {
    /// Decodes an assignment, returning an empty array if the bytes are
    /// truncated or use an unknown layout.
    ///
    /// Malformed input is not an error worth surfacing: the assignment is
    /// informational, and a future protocol version would otherwise break the
    /// whole group inspector.
    public static func decode(_ data: Data) -> [MemberAssignment] {
        guard data.count >= 6 else { return [] }
        var cursor = Cursor(data)

        guard cursor.int16() != nil, let topicCount = cursor.int32(), topicCount >= 0 else {
            return []
        }

        var assignments: [MemberAssignment] = []
        for _ in 0..<topicCount {
            guard let name = cursor.string(), let partitionCount = cursor.int32() else { return assignments }
            var partitions: [Int32] = []
            for _ in 0..<partitionCount {
                guard let partition = cursor.int32() else { return assignments }
                partitions.append(partition)
            }
            assignments.append(MemberAssignment(topic: name, partitions: partitions))
        }
        return assignments
    }

    /// Big-endian reader that returns `nil` rather than trapping at the end.
    private struct Cursor {
        private let data: Data
        private var index: Int

        init(_ data: Data) {
            self.data = data
            self.index = data.startIndex
        }

        mutating func int16() -> Int16? {
            guard let bytes = take(2) else { return nil }
            return Int16(bigEndian: bytes.withUnsafeBytes { $0.loadUnaligned(as: Int16.self) })
        }

        mutating func int32() -> Int32? {
            guard let bytes = take(4) else { return nil }
            return Int32(bigEndian: bytes.withUnsafeBytes { $0.loadUnaligned(as: Int32.self) })
        }

        mutating func string() -> String? {
            guard let length = int16(), length >= 0, let bytes = take(Int(length)) else { return nil }
            return String(decoding: bytes, as: UTF8.self)
        }

        private mutating func take(_ count: Int) -> Data? {
            guard index + count <= data.endIndex else { return nil }
            defer { index += count }
            return data[index..<(index + count)]
        }
    }
}
