import Foundation

/// What a payload looks like, as far as ``PayloadFormatter`` can tell.
///
/// This is the *detected* shape, not a guarantee that formatting succeeded: a
/// malformed document still reports `.json` or `.xml` so the UI can say the
/// value was meant to be JSON but could not be parsed.
public enum PayloadKind: String, Sendable {
    case json = "JSON"
    case xml = "XML"
    case text = "Text"
    case binary = "Binary"
    case empty = "Empty"
    case null = "Null"

    /// True when a pretty form is worth offering.
    public var isStructured: Bool { self == .json || self == .xml }
}

/// A payload prepared for display.
public struct FormattedPayload: Sendable {
    public let kind: PayloadKind
    /// Indented form, or `nil` when the payload is not structured or failed to parse.
    public let pretty: String?
    /// The payload as-is: UTF-8 text, or an annotated hex dump for binary.
    public let raw: String
    /// Why pretty-printing failed, when it did. Shown as a badge.
    public let problem: String?
    public let byteCount: Int

    /// True when the payload announced itself as structured but would not parse.
    public var isMalformed: Bool { kind.isStructured && pretty == nil }

    /// The text to show, honouring the caller's preference but falling back to
    /// raw whenever no pretty form exists.
    public func text(pretty preferPretty: Bool) -> String {
        preferPretty ? (pretty ?? raw) : raw
    }
}

/// Detects JSON and XML in record payloads and pretty-prints them.
///
/// Detection is by leading character, not by trying every parser: a 3 KB value
/// is formatted on every selection change, and Kafka payloads that begin with
/// `{`, `[` or `<` are overwhelmingly what they appear to be.
public enum PayloadFormatter {
    /// Formats a key or value.
    ///
    /// - Parameter data: raw payload. `nil` means absent (a tombstone value).
    /// - Returns: the detected kind plus raw and, where possible, pretty text.
    public static func format(_ data: Data?) -> FormattedPayload {
        guard let data else {
            return FormattedPayload(kind: .null, pretty: nil, raw: "null", problem: nil, byteCount: 0)
        }
        guard !data.isEmpty else {
            return FormattedPayload(kind: .empty, pretty: nil, raw: "", problem: nil, byteCount: 0)
        }

        // A strict UTF-8 decode is the binary test. Lossy decoding would accept
        // arbitrary bytes and render them as replacement characters.
        guard let text = String(data: data, encoding: .utf8) else {
            return FormattedPayload(
                kind: .binary,
                pretty: nil,
                raw: hexDump(data),
                problem: nil,
                byteCount: data.count
            )
        }

        switch text.first(where: { !$0.isWhitespace }) {
        case "{", "[":
            return formatJSON(data, text: text)
        case "<":
            return formatXML(data, text: text)
        default:
            return FormattedPayload(
                kind: .text,
                pretty: nil,
                raw: text,
                problem: nil,
                byteCount: data.count
            )
        }
    }

    /// Convenience for a lossy single-line summary of arbitrary bytes.
    public static func lossyText(_ data: Data) -> String {
        String(decoding: data, as: UTF8.self)
    }

    private static func formatJSON(_ data: Data, text: String) -> FormattedPayload {
        do {
            let object = try JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])
            // sortedKeys because JSONSerialization discards source order anyway;
            // sorting at least makes repeated views of a record identical.
            let pretty = try JSONSerialization.data(
                withJSONObject: object,
                options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
            )
            return FormattedPayload(
                kind: .json,
                pretty: String(decoding: pretty, as: UTF8.self),
                raw: text,
                problem: nil,
                byteCount: data.count
            )
        } catch {
            return FormattedPayload(
                kind: .json,
                pretty: nil,
                raw: text,
                problem: "Invalid JSON: \(reason(from: error))",
                byteCount: data.count
            )
        }
    }

    private static func formatXML(_ data: Data, text: String) -> FormattedPayload {
        do {
            let document = try XMLDocument(data: data, options: [.nodePreserveCDATA])
            let pretty = document.xmlString(options: [.nodePrettyPrint])
            return FormattedPayload(
                kind: .xml,
                pretty: pretty,
                raw: text,
                problem: nil,
                byteCount: data.count
            )
        } catch {
            return FormattedPayload(
                kind: .xml,
                pretty: nil,
                raw: text,
                problem: "Invalid XML: \(reason(from: error))",
                byteCount: data.count
            )
        }
    }

    /// Pulls the useful sentence out of a parser error.
    ///
    /// Both `JSONSerialization` and `XMLDocument` put the readable message in
    /// `NSDebugDescriptionErrorKey`; `localizedDescription` is generic — it
    /// says only that the data could not be read, with no position or cause.
    /// Shared with the importer, which reports the same class of problem.
    public static func reason(from error: Error) -> String {
        let info = (error as NSError).userInfo
        if let detail = info[NSDebugDescriptionErrorKey] as? String { return detail }
        if let detail = info[NSLocalizedDescriptionKey] as? String { return detail }
        return error.localizedDescription
    }

    /// Renders bytes as offset, hex and printable ASCII, 16 bytes per line.
    ///
    /// - Parameters:
    ///   - data: bytes to dump.
    ///   - limit: how many bytes to render before truncating. A record can be
    ///     megabytes, and no one reads that as hex.
    public static func hexDump(_ data: Data, limit: Int = 2048) -> String {
        let shown = data.prefix(limit)
        var lines: [String] = []

        for start in stride(from: 0, to: shown.count, by: 16) {
            let row = Array(shown[(shown.startIndex + start)..<min(shown.startIndex + start + 16, shown.endIndex)])
            let hex = row.map { String(format: "%02x", $0) }
                .chunked(into: 8)
                .map { $0.joined(separator: " ") }
                .joined(separator: "  ")
            let ascii = row.map { byte in
                (0x20...0x7e).contains(byte) ? String(UnicodeScalar(byte)) : "."
            }
            .joined()
            let paddedHex = hex.padding(toLength: max(hex.count, 50), withPad: " ", startingAt: 0)
            lines.append(String(format: "%08x  %@ |%@|", start, paddedHex, ascii))
        }

        if data.count > limit {
            lines.append("… \(data.count - limit) more bytes")
        }
        return lines.joined(separator: "\n")
    }
}

private extension Array {
    func chunked(into size: Int) -> [[Element]] {
        stride(from: 0, to: count, by: size).map { Array(self[$0..<Swift.min($0 + size, count)]) }
    }
}
