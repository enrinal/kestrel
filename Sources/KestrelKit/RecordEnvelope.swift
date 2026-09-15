import Foundation

/// A payload as it appears in a saved envelope.
///
/// Kafka keys and values are arbitrary bytes, but most are text, so the
/// encoding is tagged rather than fixed: readable payloads stay readable in the
/// file, and binary ones still round-trip exactly.
public struct EnvelopePayload: Codable, Hashable, Sendable {
    public enum Encoding: String, Codable, Sendable {
        /// `data` is the payload itself.
        case utf8
        /// `data` is Base64 of the payload.
        case base64
    }

    public let encoding: Encoding
    public let data: String

    /// Tags `payload` as UTF-8 when it decodes cleanly, Base64 otherwise.
    public init(_ payload: Data) {
        if let text = String(data: payload, encoding: .utf8) {
            self.encoding = .utf8
            self.data = text
        } else {
            self.encoding = .base64
            self.data = payload.base64EncodedString()
        }
    }

    public init(encoding: Encoding, data: String) {
        self.encoding = encoding
        self.data = data
    }

    /// The original bytes, or `nil` if `data` is not valid for its encoding.
    public var bytes: Data? {
        switch encoding {
        case .utf8: Data(data.utf8)
        case .base64: Data(base64Encoded: data)
        }
    }
}

/// A header in a saved envelope.
public struct EnvelopeHeader: Codable, Hashable, Sendable {
    public let name: String
    /// `null` for a header with no value, which Kafka allows.
    public let value: EnvelopePayload?

    public init(name: String, value: EnvelopePayload?) {
        self.name = name
        self.value = value
    }
}

/// A record and its metadata, in a form that survives a trip through a file.
///
/// This is the interchange format for saving one record and, from slice 11, for
/// importing records back. Only `value` and `headers` are needed to produce a
/// record; `partition`, `offset` and the timestamps describe where a saved
/// record came from and are ignored on import.
public struct RecordEnvelope: Codable, Hashable, Sendable {
    /// Format version, always written, used to tell a saved envelope from any
    /// other JSON document when importing. Without it a payload that happens
    /// to have a `value` field would be read as an envelope.
    public var envelopeVersion: Int = RecordEnvelope.currentVersion
    public var topic: String?
    public var partition: Int32?
    public var offset: Int64?
    /// Kafka's own timestamp, in milliseconds since the epoch. Authoritative.
    public var timestampMillis: Int64?
    /// Informational copy of `timestampMillis` for anyone reading the file.
    /// Ignored when importing, so the two can never disagree in effect.
    public var timestamp: String?
    /// `null` distinguishes a keyless record from one with an empty key.
    public var key: EnvelopePayload?
    /// `null` marks a tombstone, which is not the same as an empty value.
    public var value: EnvelopePayload?
    public var headers: [EnvelopeHeader]

    /// Version this build writes.
    public static let currentVersion = 1

    public init(
        topic: String? = nil,
        partition: Int32? = nil,
        offset: Int64? = nil,
        timestampMillis: Int64? = nil,
        timestamp: String? = nil,
        key: EnvelopePayload? = nil,
        value: EnvelopePayload? = nil,
        headers: [EnvelopeHeader] = []
    ) {
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestampMillis = timestampMillis
        self.timestamp = timestamp
        self.key = key
        self.value = value
        self.headers = headers
    }

    /// Describes an existing record.
    public init(record: KafkaRecord, topic: String) {
        self.topic = topic
        self.partition = record.partition
        self.offset = record.offset
        self.timestampMillis = record.timestamp.map { Int64(($0.timeIntervalSince1970 * 1000).rounded()) }
        // Plain .iso8601; adding a field modifier such as timeZone(separator:)
        // replaces the whole style and yields just that field.
        self.timestamp = record.timestamp.map { $0.formatted(.iso8601) }
        self.key = record.key.map(EnvelopePayload.init)
        self.value = record.value.map(EnvelopePayload.init)
        self.headers = record.headers.map {
            EnvelopeHeader(name: $0.name, value: $0.value.map(EnvelopePayload.init))
        }
    }

    /// The key bytes, or `nil` for a keyless record.
    public var keyBytes: Data? { key?.bytes }

    /// The value bytes, or `nil` for a tombstone.
    public var valueBytes: Data? { value?.bytes }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        // Decoded leniently so an envelope saved before the marker existed, or
        // hand-written by someone, still loads.
        envelopeVersion = try container.decodeIfPresent(Int.self, forKey: .envelopeVersion)
            ?? RecordEnvelope.currentVersion
        topic = try container.decodeIfPresent(String.self, forKey: .topic)
        partition = try container.decodeIfPresent(Int32.self, forKey: .partition)
        offset = try container.decodeIfPresent(Int64.self, forKey: .offset)
        timestampMillis = try container.decodeIfPresent(Int64.self, forKey: .timestampMillis)
        timestamp = try container.decodeIfPresent(String.self, forKey: .timestamp)
        key = try container.decodeIfPresent(EnvelopePayload.self, forKey: .key)
        value = try container.decodeIfPresent(EnvelopePayload.self, forKey: .value)
        headers = try container.decodeIfPresent([EnvelopeHeader].self, forKey: .headers) ?? []
    }

    /// Headers in the form the producer takes.
    public var recordHeaders: [RecordHeader] {
        headers.map { RecordHeader(name: $0.name, value: $0.value?.bytes) }
    }
}

/// How a saved record is laid out on disk.
public enum RecordSaveFormat: String, CaseIterable, Sendable {
    /// The value bytes alone, byte for byte, with no wrapper.
    case rawValue
    /// A ``RecordEnvelope`` as pretty-printed JSON.
    case jsonEnvelope

    public var label: String {
        switch self {
        case .rawValue: "Raw value bytes"
        case .jsonEnvelope: "JSON envelope"
        }
    }

    /// The extension to suggest, given the value being saved.
    ///
    /// The envelope adds an `envelope` component so that saving both formats of
    /// one record does not suggest the same filename twice — a JSON value would
    /// otherwise make each format propose `topic-p0-1.json`, and the second save
    /// would silently overwrite the first.
    ///
    /// - Parameter value: the record's value, used to tell JSON from other text.
    public func suggestedExtension(for value: Data?) -> String {
        switch self {
        case .jsonEnvelope:
            return "envelope.json"
        case .rawValue:
            guard let value else { return "bin" }
            return switch PayloadFormatter.format(value).kind {
            case .json: "json"
            case .xml: "xml"
            case .text: "txt"
            case .binary, .empty, .null: "bin"
            }
        }
    }
}

/// Writes records to disk and reads envelopes back.
public enum RecordFile {
    /// JSON with sorted keys and indentation, so saved files diff cleanly.
    private static var encoder: JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        return encoder
    }

    /// Encodes a record in the requested format.
    ///
    /// - Parameters:
    ///   - record: record to encode.
    ///   - topic: topic it came from, recorded in the envelope.
    ///   - format: layout to produce.
    /// - Returns: the bytes to write. For ``RecordSaveFormat/rawValue`` these
    ///   are exactly the record's value bytes, so the file round-trips; a
    ///   tombstone therefore writes an empty file.
    public static func encode(
        record: KafkaRecord,
        topic: String,
        format: RecordSaveFormat
    ) throws -> Data {
        switch format {
        case .rawValue:
            return record.value ?? Data()
        case .jsonEnvelope:
            return try encoder.encode(RecordEnvelope(record: record, topic: topic))
        }
    }

    /// Writes a record to `url`, replacing any existing file.
    @discardableResult
    public static func write(
        record: KafkaRecord,
        topic: String,
        format: RecordSaveFormat,
        to url: URL
    ) throws -> Int {
        let data = try encode(record: record, topic: topic, format: format)
        try data.write(to: url, options: .atomic)
        return data.count
    }

    /// Reads a saved envelope back.
    ///
    /// - Throws: a decoding error if the file is not a ``RecordEnvelope``.
    public static func readEnvelope(from url: URL) throws -> RecordEnvelope {
        try JSONDecoder().decode(RecordEnvelope.self, from: Data(contentsOf: url))
    }
}

public extension RecordSaveFormat {
    /// Builds the filename the save panel suggests.
    ///
    /// Lives here so the naming rule — in particular that two formats never
    /// suggest the same name — can be checked without a save panel.
    ///
    /// - Parameters:
    ///   - record: record being saved.
    ///   - topic: topic it came from.
    ///   - format: layout being written.
    static func suggestedNameForChecks(
        record: KafkaRecord,
        topic: String,
        format: RecordSaveFormat
    ) -> String {
        let safeTopic = topic
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: ":", with: "_")
        return "\(safeTopic)-p\(record.partition)-\(record.offset).\(format.suggestedExtension(for: record.value))"
    }
}
