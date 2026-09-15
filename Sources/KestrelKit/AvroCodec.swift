import Foundation

/// Why an Avro payload could not be read or written.
public enum AvroCodecError: LocalizedError, Equatable {
    case truncated(String)
    case unknownReference(String)
    case badUnionIndex(Int, count: Int)
    case badEnumIndex(Int, count: Int)
    case valueMismatch(field: String, expected: String)
    case trailingBytes(Int)

    public var errorDescription: String? {
        switch self {
        case .truncated(let what):
            "Payload ended while reading \(what)"
        case .unknownReference(let name):
            "Schema refers to an unknown type: \(name)"
        case .badUnionIndex(let index, let count):
            "Union branch \(index) is out of range for \(count) options"
        case .badEnumIndex(let index, let count):
            "Enum symbol \(index) is out of range for \(count) symbols"
        case .valueMismatch(let field, let expected):
            // The article belongs with the expected value, not the
            // template: "is not a one of the union's branches" is what a
            // hardcoded "a" produces.
            "Value for \(field) is not \(expected)"
        case .trailingBytes(let count):
            "\(count) byte(s) left over after the record, so the schema does not fit the data"
        }
    }
}

/// Reads Avro's binary encoding into JSON-shaped Swift values.
///
/// Decoding produces `Any` in the shapes `JSONSerialization` expects, so the
/// result can be pretty-printed with the same code path as any other JSON
/// payload. A union is rendered as its branch's value rather than Avro's
/// `{"type": value}` JSON encoding, because the point here is readability.
public struct AvroDecoder {
    private let named: [String: AvroSchema]

    public init(named: [String: AvroSchema] = [:]) {
        self.named = named
    }

    /// Decodes one value.
    ///
    /// - Parameters:
    ///   - data: the Avro body, with any Confluent header already removed.
    ///   - schema: the writer's schema.
    /// - Returns: the value, as JSON-compatible Swift types.
    /// - Throws: ``AvroCodecError`` when the bytes and the schema disagree,
    ///   including bytes left over, which means the wrong schema was used.
    public func decode(_ data: Data, as schema: AvroSchema) throws -> Any {
        var cursor = Cursor(data)
        let value = try read(schema, from: &cursor)
        guard cursor.isAtEnd else {
            throw AvroCodecError.trailingBytes(cursor.remaining)
        }
        return value
    }

    private func read(_ schema: AvroSchema, from cursor: inout Cursor) throws -> Any {
        switch schema {
        case .null:
            return NSNull()

        case .boolean:
            return try cursor.byte("a boolean") != 0

        case .int(let logical):
            let value = try cursor.zigzag("an int")
            return format(int: value, logical: logical)

        case .long(let logical):
            let value = try cursor.zigzag("a long")
            return format(int: value, logical: logical)

        case .float:
            return Double(Float(bitPattern: try cursor.fixedWidth(4, "a float")))

        case .double:
            return Double(bitPattern: try cursor.fixedWidth(8, "a double"))

        case .bytes(let logical, let scale):
            let length = Int(try cursor.zigzag("a byte length"))
            let bytes = try cursor.take(length, "bytes")
            if logical == .decimal {
                return decimal(bytes, scale: scale)
            }
            // Bytes are rendered as base64: they are not text, and inventing
            // text for them would misrepresent the record.
            return bytes.base64EncodedString()

        case .string(let logical):
            let length = Int(try cursor.zigzag("a string length"))
            let bytes = try cursor.take(length, "a string")
            let text = String(data: bytes, encoding: .utf8) ?? bytes.base64EncodedString()
            return logical == .uuid ? text : text

        case .fixed(_, let size, let logical, let scale):
            let bytes = try cursor.take(size, "a fixed")
            if logical == .decimal { return decimal(bytes, scale: scale) }
            return bytes.base64EncodedString()

        case .enumeration(_, let symbols):
            let index = Int(try cursor.zigzag("an enum"))
            guard symbols.indices.contains(index) else {
                throw AvroCodecError.badEnumIndex(index, count: symbols.count)
            }
            return symbols[index]

        case .array(let items):
            var values: [Any] = []
            try readBlocks(from: &cursor) {
                values.append(try read(items, from: &$0))
            }
            return values

        case .map(let values):
            var entries: [String: Any] = [:]
            try readBlocks(from: &cursor) { cursor in
                let length = Int(try cursor.zigzag("a map key length"))
                let key = String(data: try cursor.take(length, "a map key"), encoding: .utf8) ?? ""
                entries[key] = try read(values, from: &cursor)
            }
            return entries

        case .union(let options):
            let index = Int(try cursor.zigzag("a union branch"))
            guard options.indices.contains(index) else {
                throw AvroCodecError.badUnionIndex(index, count: options.count)
            }
            return try read(options[index], from: &cursor)

        case .record(_, let fields):
            var object: [String: Any] = [:]
            for field in fields {
                object[field.name] = try read(field.schema, from: &cursor)
            }
            return object

        case .reference(let name):
            guard let resolved = named[name], resolved != .reference(name: name) else {
                throw AvroCodecError.unknownReference(name)
            }
            return try read(resolved, from: &cursor)
        }
    }

    /// Reads an array or map's blocks until the terminating zero count.
    private func readBlocks(
        from cursor: inout Cursor,
        item: (inout Cursor) throws -> Void
    ) throws {
        while true {
            var count = try cursor.zigzag("a block count")
            if count == 0 { return }
            if count < 0 {
                // A negative count is followed by the block's size in bytes,
                // which is only useful for skipping, so it is read and dropped.
                count = -count
                _ = try cursor.zigzag("a block size")
            }
            for _ in 0..<count {
                try item(&cursor)
            }
        }
    }

    /// Renders an integer, applying its logical type.
    ///
    /// Dates and timestamps become readable strings; anything else stays a
    /// number, since a reader of the JSON will want to compute with it.
    private func format(int value: Int64, logical: AvroLogicalType?) -> Any {
        switch logical {
        case .date:
            // Spelled out field by field: the modifiers on `.iso8601` replace
            // the format rather than adding to it, so asking for a date by
            // trimming a full timestamp yields the time instead.
            let date = Date(timeIntervalSince1970: Double(value) * 86_400)
            return date.formatted(.iso8601.year().month().day().dateSeparator(.dash))
        case .timestampMillis:
            return Date(timeIntervalSince1970: Double(value) / 1000).formatted(.iso8601)
        case .timestampMicros:
            return Date(timeIntervalSince1970: Double(value) / 1_000_000).formatted(.iso8601)
        case .timeMillis, .timeMicros:
            return value
        default:
            return value
        }
    }

    /// Renders a decimal as a string, so no precision is lost on the way to
    /// JSON, where it would otherwise become a `Double`.
    private func decimal(_ bytes: Data, scale: Int) -> String {
        guard !bytes.isEmpty else { return "0" }

        // Two's complement, big-endian, of arbitrary length.
        let negative = bytes[bytes.startIndex] & 0x80 != 0
        var magnitude = [UInt8](negative ? Data(bytes.map { ~$0 }) : bytes)
        if negative {
            // Add one, to finish the two's complement negation.
            var index = magnitude.count - 1
            while index >= 0 {
                if magnitude[index] == 0xff {
                    magnitude[index] = 0
                    index -= 1
                } else {
                    magnitude[index] += 1
                    break
                }
            }
        }

        var digits = "0"
        for byte in magnitude {
            digits = Self.multiply(digits, by: 256, adding: Int(byte))
        }

        if scale > 0 {
            var padded = digits
            while padded.count <= scale {
                padded = "0" + padded
            }
            let split = padded.index(padded.endIndex, offsetBy: -scale)
            digits = "\(padded[..<split]).\(padded[split...])"
        }

        return negative ? "-\(digits)" : digits
    }

    /// Decimal string arithmetic, so a 128-bit unscaled value does not have to
    /// fit in an `Int64`.
    private static func multiply(_ number: String, by factor: Int, adding addend: Int) -> String {
        var carry = addend
        var result: [Character] = []

        for character in number.reversed() {
            let digit = Int(String(character)) ?? 0
            let product = digit * factor + carry
            result.append(Character(String(product % 10)))
            carry = product / 10
        }
        while carry > 0 {
            result.append(Character(String(carry % 10)))
            carry /= 10
        }

        let text = String(result.reversed())
        let trimmed = text.drop { $0 == "0" }
        return trimmed.isEmpty ? "0" : String(trimmed)
    }

    /// Walks a byte buffer, reporting where it ran out.
    private struct Cursor {
        private let data: Data
        private var index: Data.Index

        init(_ data: Data) {
            self.data = data
            self.index = data.startIndex
        }

        var isAtEnd: Bool { index >= data.endIndex }
        var remaining: Int { data.distance(from: index, to: data.endIndex) }

        mutating func byte(_ what: String) throws -> UInt8 {
            guard index < data.endIndex else { throw AvroCodecError.truncated(what) }
            defer { index = data.index(after: index) }
            return data[index]
        }

        mutating func take(_ count: Int, _ what: String) throws -> Data {
            guard count >= 0, remaining >= count else { throw AvroCodecError.truncated(what) }
            let end = data.index(index, offsetBy: count)
            defer { index = end }
            return Data(data[index..<end])
        }

        /// Reads a variable-length zigzag integer, Avro's encoding for int and
        /// long alike.
        mutating func zigzag(_ what: String) throws -> Int64 {
            var result: UInt64 = 0
            var shift: UInt64 = 0

            while true {
                let byte = try byte(what)
                result |= UInt64(byte & 0x7f) << shift
                if byte & 0x80 == 0 { break }
                shift += 7
                guard shift < 64 else { throw AvroCodecError.truncated(what) }
            }

            return Int64(bitPattern: (result >> 1) ^ (0 &- (result & 1)))
        }

        /// Reads a little-endian fixed-width value, as floats and doubles use.
        mutating func fixedWidth<T: FixedWidthInteger & UnsignedInteger>(
            _ count: Int,
            _ what: String
        ) throws -> T {
            let bytes = try take(count, what)
            return bytes.reversed().reduce(T(0)) { ($0 << 8) | T($1) }
        }
    }
}

/// Writes JSON-shaped values in Avro's binary encoding.
///
/// Only what producing needs: the value comes from what someone typed into the
/// produce sheet, parsed as JSON, so the input is always JSON-shaped.
public struct AvroEncoder {
    private let named: [String: AvroSchema]

    public init(named: [String: AvroSchema] = [:]) {
        self.named = named
    }

    /// Encodes a value against a schema.
    ///
    /// - Parameters:
    ///   - value: JSON-compatible value, as `JSONSerialization` produces.
    ///   - schema: schema to encode against.
    /// - Returns: the Avro body, with no Confluent header.
    /// - Throws: ``AvroCodecError/valueMismatch(field:expected:)`` naming the
    ///   field that does not fit, so the message says where to look.
    public func encode(_ value: Any, as schema: AvroSchema) throws -> Data {
        var output = Data()
        try write(value, as: schema, path: "value", into: &output)
        return output
    }

    private func write(
        _ value: Any,
        as schema: AvroSchema,
        path: String,
        into output: inout Data
    ) throws {
        switch schema {
        case .null:
            guard value is NSNull else {
                throw AvroCodecError.valueMismatch(field: path, expected: "null")
            }

        case .boolean:
            guard let flag = value as? Bool else {
                throw AvroCodecError.valueMismatch(field: path, expected: "a boolean")
            }
            output.append(flag ? 1 : 0)

        case .int, .long:
            guard let number = Self.integer(value) else {
                throw AvroCodecError.valueMismatch(field: path, expected: "a whole number")
            }
            output.append(Self.zigzag(number))

        case .float:
            guard let number = Self.double(value) else {
                throw AvroCodecError.valueMismatch(field: path, expected: "a number")
            }
            output.append(Self.littleEndian(UInt32(Float(number).bitPattern), width: 4))

        case .double:
            guard let number = Self.double(value) else {
                throw AvroCodecError.valueMismatch(field: path, expected: "a number")
            }
            output.append(Self.littleEndian(number.bitPattern, width: 8))

        case .bytes:
            guard let text = value as? String, let bytes = Data(base64Encoded: text) else {
                throw AvroCodecError.valueMismatch(field: path, expected: "a base64 string")
            }
            output.append(Self.zigzag(Int64(bytes.count)))
            output.append(bytes)

        case .string:
            guard let text = value as? String else {
                throw AvroCodecError.valueMismatch(field: path, expected: "a string")
            }
            let bytes = Data(text.utf8)
            output.append(Self.zigzag(Int64(bytes.count)))
            output.append(bytes)

        case .fixed(_, let size, _, _):
            guard let text = value as? String, let bytes = Data(base64Encoded: text),
                  bytes.count == size
            else {
                throw AvroCodecError.valueMismatch(
                    field: path,
                    expected: "a base64 string of \(size) bytes"
                )
            }
            output.append(bytes)

        case .enumeration(_, let symbols):
            guard let text = value as? String, let index = symbols.firstIndex(of: text) else {
                throw AvroCodecError.valueMismatch(
                    field: path,
                    expected: "one of \(symbols.joined(separator: ", "))"
                )
            }
            output.append(Self.zigzag(Int64(index)))

        case .array(let items):
            guard let values = value as? [Any] else {
                throw AvroCodecError.valueMismatch(field: path, expected: "an array")
            }
            if !values.isEmpty {
                output.append(Self.zigzag(Int64(values.count)))
                for (index, item) in values.enumerated() {
                    try write(item, as: items, path: "\(path)[\(index)]", into: &output)
                }
            }
            output.append(Self.zigzag(0))

        case .map(let values):
            guard let entries = value as? [String: Any] else {
                throw AvroCodecError.valueMismatch(field: path, expected: "an object")
            }
            if !entries.isEmpty {
                output.append(Self.zigzag(Int64(entries.count)))
                // Sorted so the same input always encodes to the same bytes.
                for key in entries.keys.sorted() {
                    let keyBytes = Data(key.utf8)
                    output.append(Self.zigzag(Int64(keyBytes.count)))
                    output.append(keyBytes)
                    try write(entries[key]!, as: values, path: "\(path).\(key)", into: &output)
                }
            }
            output.append(Self.zigzag(0))

        case .union(let options):
            // The first branch the value fits. Nulls are placed by matching
            // the null branch, which is why order matters in Avro unions.
            for (index, option) in options.enumerated() {
                var attempt = Data()
                guard (try? write(value, as: option, path: path, into: &attempt)) != nil else {
                    continue
                }
                output.append(Self.zigzag(Int64(index)))
                output.append(attempt)
                return
            }
            throw AvroCodecError.valueMismatch(field: path, expected: "one of the union's branches")

        case .record(_, let fields):
            guard let object = value as? [String: Any] else {
                throw AvroCodecError.valueMismatch(field: path, expected: "an object")
            }
            for field in fields {
                guard let fieldValue = object[field.name] else {
                    throw AvroCodecError.valueMismatch(
                        field: "\(path).\(field.name)",
                        expected: "a present field"
                    )
                }
                try write(
                    fieldValue,
                    as: field.schema,
                    path: "\(path).\(field.name)",
                    into: &output
                )
            }

        case .reference(let name):
            guard let resolved = named[name], resolved != .reference(name: name) else {
                throw AvroCodecError.unknownReference(name)
            }
            try write(value, as: resolved, path: path, into: &output)
        }
    }

    private static func integer(_ value: Any) -> Int64? {
        switch value {
        case let number as Int: Int64(number)
        case let number as Int64: number
        case let number as NSNumber:
            // A JSON number arrives as NSNumber; only accept it if it really is
            // whole, so 1.5 is not silently truncated to 1.
            number.doubleValue == number.doubleValue.rounded() ? number.int64Value : nil
        default: nil
        }
    }

    private static func double(_ value: Any) -> Double? {
        switch value {
        case let number as Double: number
        case let number as Int: Double(number)
        case let number as NSNumber: number.doubleValue
        default: nil
        }
    }

    private static func zigzag(_ value: Int64) -> Data {
        var encoded = UInt64(bitPattern: (value << 1) ^ (value >> 63))
        var data = Data()
        repeat {
            var byte = UInt8(encoded & 0x7f)
            encoded >>= 7
            if encoded != 0 { byte |= 0x80 }
            data.append(byte)
        } while encoded != 0
        return data
    }

    private static func littleEndian<T: FixedWidthInteger>(_ value: T, width: Int) -> Data {
        var data = Data()
        for shift in 0..<width {
            data.append(UInt8((value >> (shift * 8)) & 0xff))
        }
        return data
    }
}
