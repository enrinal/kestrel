import Foundation

/// Writes a command's results, as text for a person or JSON for a script.
///
/// Both forms come from the same call so a command cannot drift into supporting
/// one and not the other.
struct Output {
    let wantsJSON: Bool

    /// Prints a table, or the same rows as a JSON array of objects.
    ///
    /// - Parameters:
    ///   - columns: column headings, in order.
    ///   - rows: cells per row, matching `columns`.
    ///   - keys: JSON field names, when they should differ from the headings.
    func table(columns: [String], rows: [[String]], keys: [String]? = nil) {
        guard wantsJSON else {
            printTable(columns: columns, rows: rows)
            return
        }

        let names = keys ?? columns.map(Self.jsonKey)
        let objects = rows.map { row in
            Dictionary(uniqueKeysWithValues: zip(names, row).map { ($0, $1) })
        }
        printJSON(objects, order: names)
    }

    /// Prints key/value detail, as aligned lines or one JSON object.
    func fields(_ pairs: [(String, String)]) {
        guard wantsJSON else {
            let width = pairs.map(\.0.count).max() ?? 0
            for (name, value) in pairs {
                print("\(name.padding(toLength: width, withPad: " ", startingAt: 0))  \(value)")
            }
            return
        }

        let names = pairs.map { Self.jsonKey($0.0) }
        printJSON(
            [Dictionary(uniqueKeysWithValues: zip(names, pairs.map(\.1)))],
            order: names,
            single: true
        )
    }

    /// A note for a person, suppressed under `--json` so the output stays
    /// machine-readable. Progress and confirmations go here.
    func note(_ text: String) {
        guard !wantsJSON else { return }
        print(text)
    }

    /// A line that is part of the result and so is printed either way.
    func line(_ text: String) {
        print(text)
    }

    private func printTable(columns: [String], rows: [[String]]) {
        guard !rows.isEmpty else {
            print("(none)")
            return
        }

        var widths = columns.map(\.count)
        for row in rows {
            for (index, cell) in row.enumerated() where index < widths.count {
                widths[index] = max(widths[index], cell.count)
            }
        }

        // The last column is not padded: trailing spaces are invisible and
        // break `diff` against other tools' output.
        func format(_ cells: [String]) -> String {
            cells.enumerated()
                .map { index, cell in
                    index == cells.count - 1
                        ? cell
                        : cell.padding(toLength: widths[index], withPad: " ", startingAt: 0)
                }
                .joined(separator: "  ")
        }

        print(format(columns))
        print(format(widths.map { String(repeating: "-", count: $0) }))
        for row in rows {
            print(format(row))
        }
    }

    /// Serialises with sorted keys and a stable field order.
    private func printJSON(_ objects: [[String: String]], order: [String], single: Bool = false) {
        // Built by hand rather than with JSONSerialization, which sorts or
        // randomises keys; a script reading the output should see the columns
        // in the order the table shows them.
        func encode(_ object: [String: String]) -> String {
            let fields = order.compactMap { key -> String? in
                guard let value = object[key] else { return nil }
                return "    \"\(key)\": \(Self.quote(value))"
            }
            return "  {\n\(fields.joined(separator: ",\n"))\n  }"
        }

        if single, let first = objects.first {
            print(encode(first).trimmingCharacters(in: .whitespaces))
            return
        }
        print("[\n\(objects.map(encode).joined(separator: ",\n"))\n]")
    }

    /// JSON-quotes a string, escaping what the format requires.
    static func quote(_ text: String) -> String {
        var escaped = ""
        for character in text.unicodeScalars {
            switch character {
            case "\"": escaped += "\\\""
            case "\\": escaped += "\\\\"
            case "\n": escaped += "\\n"
            case "\r": escaped += "\\r"
            case "\t": escaped += "\\t"
            default:
                if character.value < 0x20 {
                    escaped += String(format: "\\u%04x", character.value)
                } else {
                    escaped.unicodeScalars.append(character)
                }
            }
        }
        return "\"\(escaped)\""
    }

    /// Turns a column heading into a JSON field name: `Log End` → `logEnd`.
    static func jsonKey(_ heading: String) -> String {
        let words = heading.split(separator: " ").map(String.init)
        guard let first = words.first else { return heading }
        return ([first.lowercased()] + words.dropFirst().map(\.capitalized)).joined()
    }
}

/// Writes a message to standard error, for anything that is not the result.
func printError(_ text: String) {
    FileHandle.standardError.write(Data("kestrel: \(text)\n".utf8))
}
