import Foundation

/// The Confluent wire format that wraps a schema id around a payload.
///
/// Every Confluent-serialised record starts with a zero byte, then the schema's
/// id as four big-endian bytes, then the encoded payload. The magic byte is
/// what makes the format recognisable at all, so a record can be identified as
/// Avro without knowing anything about the topic.
public enum ConfluentWireFormat {
    /// The byte every Confluent payload starts with.
    public static let magicByte: UInt8 = 0

    /// The five bytes of header before the payload begins.
    public static let headerSize = 5

    /// Splits a payload into its schema id and body.
    ///
    /// - Parameter payload: the record's bytes.
    /// - Returns: the schema id and the payload after the header, or `nil` if
    ///   this is not Confluent-framed. A payload of exactly five bytes is
    ///   framed with an empty body, which is legitimate for a schema whose
    ///   encoding is zero bytes.
    public static func split(_ payload: Data) -> (schemaID: Int32, body: Data)? {
        guard payload.count >= headerSize, payload[payload.startIndex] == magicByte else {
            return nil
        }

        let idBytes = payload[payload.index(payload.startIndex, offsetBy: 1)...]
            .prefix(4)
        let id = idBytes.reduce(Int32(0)) { ($0 << 8) | Int32($1) }
        return (id, Data(payload.dropFirst(headerSize)))
    }

    /// Wraps a body in the header for `schemaID`.
    public static func frame(schemaID: Int32, body: Data) -> Data {
        var data = Data([magicByte])
        data.append(UInt8((schemaID >> 24) & 0xff))
        data.append(UInt8((schemaID >> 16) & 0xff))
        data.append(UInt8((schemaID >> 8) & 0xff))
        data.append(UInt8(schemaID & 0xff))
        data.append(body)
        return data
    }
}

/// An Avro logical type, which gives meaning to the primitive underneath.
public enum AvroLogicalType: String, Sendable, Equatable {
    case date
    case timeMillis = "time-millis"
    case timeMicros = "time-micros"
    case timestampMillis = "timestamp-millis"
    case timestampMicros = "timestamp-micros"
    case uuid
    case decimal
}

/// An Avro schema, as much of it as Kestrel reads and writes.
///
/// Named types are kept by name so a schema can refer back to one it has
/// already defined, which is how recursive types are written.
public indirect enum AvroSchema: Sendable, Equatable {
    case null
    case boolean
    case int(logical: AvroLogicalType?)
    case long(logical: AvroLogicalType?)
    case float
    case double
    case bytes(logical: AvroLogicalType?, scale: Int)
    case string(logical: AvroLogicalType?)
    case record(name: String, fields: [Field])
    case enumeration(name: String, symbols: [String])
    case array(items: AvroSchema)
    case map(values: AvroSchema)
    case union(options: [AvroSchema])
    case fixed(name: String, size: Int, logical: AvroLogicalType?, scale: Int)
    /// A reference to a named type defined earlier in the same schema.
    case reference(name: String)

    /// One field of a record.
    public struct Field: Sendable, Equatable {
        public let name: String
        public let schema: AvroSchema
        /// Present when the writer's schema gives a default, which matters
        /// only for reading data written against a different schema.
        public let hasDefault: Bool

        public init(name: String, schema: AvroSchema, hasDefault: Bool = false) {
            self.name = name
            self.schema = schema
            self.hasDefault = hasDefault
        }
    }

    /// The name this type is known by, for named types.
    public var name: String? {
        switch self {
        case .record(let name, _), .enumeration(let name, _), .fixed(let name, _, _, _):
            name
        case .reference(let name):
            name
        default:
            nil
        }
    }
}

/// Why a schema could not be read.
public enum AvroSchemaError: LocalizedError, Equatable {
    case notJSON(String)
    case unsupportedType(String)
    case malformed(String)

    public var errorDescription: String? {
        switch self {
        case .notJSON(let detail):
            "Schema is not valid JSON: \(detail)"
        case .unsupportedType(let name):
            "Unsupported Avro type: \(name)"
        case .malformed(let detail):
            "Schema is malformed: \(detail)"
        }
    }
}

/// Reads an Avro schema document into an ``AvroSchema``.
public enum AvroSchemaParser {
    /// Parses a schema, and collects every named type it defines.
    ///
    /// - Parameter text: the schema as JSON, as Schema Registry stores it.
    /// - Returns: the schema, and a table of named types for resolving
    ///   references while decoding.
    public static func parse(
        _ text: String
    ) throws -> (schema: AvroSchema, named: [String: AvroSchema]) {
        let object: Any
        do {
            object = try JSONSerialization.jsonObject(
                with: Data(text.utf8),
                options: [.fragmentsAllowed]
            )
        } catch {
            throw AvroSchemaError.notJSON(PayloadFormatter.reason(from: error))
        }

        var named: [String: AvroSchema] = [:]
        let schema = try parse(node: object, named: &named)
        return (schema, named)
    }

    private static func parse(node: Any, named: inout [String: AvroSchema]) throws -> AvroSchema {
        // A bare string is either a primitive or a reference to a named type.
        if let name = node as? String {
            return try primitive(name, logical: nil, named: named)
        }

        // An array is a union.
        if let options = node as? [Any] {
            return .union(options: try options.map { try parse(node: $0, named: &named) })
        }

        guard let object = node as? [String: Any] else {
            throw AvroSchemaError.malformed("expected a type, got \(type(of: node))")
        }
        guard let type = object["type"] as? String else {
            // A union can also appear as the value of "type".
            if let nested = object["type"] {
                return try parse(node: nested, named: &named)
            }
            throw AvroSchemaError.malformed("a schema object needs a type")
        }

        let logical = (object["logicalType"] as? String).flatMap(AvroLogicalType.init(rawValue:))
        let scale = object["scale"] as? Int ?? 0

        switch type {
        case "record", "error":
            guard let name = object["name"] as? String else {
                throw AvroSchemaError.malformed("a record needs a name")
            }
            let fullName = [object["namespace"] as? String, name]
                .compactMap { $0 }
                .joined(separator: ".")

            // Registered before its fields are read, so a field can refer back
            // to the record it belongs to.
            named[fullName] = .reference(name: fullName)
            named[name] = .reference(name: fullName)

            guard let rawFields = object["fields"] as? [[String: Any]] else {
                throw AvroSchemaError.malformed("record \(name) has no fields")
            }
            let fields = try rawFields.map { field -> AvroSchema.Field in
                guard let fieldName = field["name"] as? String, let fieldType = field["type"] else {
                    throw AvroSchemaError.malformed("a field of \(name) is missing its name or type")
                }
                return AvroSchema.Field(
                    name: fieldName,
                    schema: try parse(node: fieldType, named: &named),
                    hasDefault: field["default"] != nil
                )
            }

            let schema = AvroSchema.record(name: fullName, fields: fields)
            named[fullName] = schema
            named[name] = schema
            return schema

        case "enum":
            guard let name = object["name"] as? String,
                  let symbols = object["symbols"] as? [String]
            else {
                throw AvroSchemaError.malformed("an enum needs a name and symbols")
            }
            let schema = AvroSchema.enumeration(name: name, symbols: symbols)
            named[name] = schema
            return schema

        case "array":
            guard let items = object["items"] else {
                throw AvroSchemaError.malformed("an array needs items")
            }
            return .array(items: try parse(node: items, named: &named))

        case "map":
            guard let values = object["values"] else {
                throw AvroSchemaError.malformed("a map needs values")
            }
            return .map(values: try parse(node: values, named: &named))

        case "fixed":
            guard let name = object["name"] as? String, let size = object["size"] as? Int else {
                throw AvroSchemaError.malformed("a fixed needs a name and size")
            }
            let schema = AvroSchema.fixed(name: name, size: size, logical: logical, scale: scale)
            named[name] = schema
            return schema

        default:
            return try primitive(type, logical: logical, scale: scale, named: named)
        }
    }

    private static func primitive(
        _ name: String,
        logical: AvroLogicalType?,
        scale: Int = 0,
        named: [String: AvroSchema]
    ) throws -> AvroSchema {
        switch name {
        case "null": return .null
        case "boolean": return .boolean
        case "int": return .int(logical: logical)
        case "long": return .long(logical: logical)
        case "float": return .float
        case "double": return .double
        case "bytes": return .bytes(logical: logical, scale: scale)
        case "string": return .string(logical: logical)
        default:
            // Not a primitive, so it must name a type defined elsewhere.
            guard named[name] != nil else {
                throw AvroSchemaError.unsupportedType(name)
            }
            return .reference(name: name)
        }
    }
}
