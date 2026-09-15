import Foundation

/// Why a command line could not be used.
///
/// Usage problems are separated from runtime failures so they can exit with a
/// different code: a script can tell "I typed it wrong" from "the broker said
/// no" without parsing messages.
struct UsageError: Error, CustomStringConvertible {
    let description: String

    init(_ description: String) {
        self.description = description
    }
}

/// A parsed command line: the words, and the options.
///
/// Hand-rolled rather than swift-argument-parser, because this package has no
/// external dependencies and adding one for eight subcommands is not worth the
/// resolution step. It understands `--flag`, `--key value`, `--key=value` and
/// bare positional words, which is all the commands need.
struct Arguments {
    /// Words that are not options, in order. The first is the command.
    private(set) var positional: [String] = []
    private var options: [String: String] = [:]
    private var flags: Set<String> = []
    /// Options that were read, so an unrecognised one can be reported rather
    /// than silently ignored — the failure mode where `--parition 3` quietly
    /// reads partition 0.
    private var consumed: Set<String> = []

    /// Options that take no value, and so must not swallow the next word.
    ///
    /// Without this list, `--json topics` would read "topics" as the value of
    /// `--json` and then find no command.
    private static let knownFlags: Set<String> = [
        "json", "help", "h", "regex", "case-sensitive", "keys-only", "values-only",
        "internal", "verbose", "tombstone", "no-headers", "include-tasks", "only-failed",
        "no-keychain"
    ]

    init(_ arguments: [String]) throws {
        var index = 0
        while index < arguments.count {
            let argument = arguments[index]
            index += 1

            guard argument.hasPrefix("--") || (argument.hasPrefix("-") && argument.count == 2) else {
                positional.append(argument)
                continue
            }

            let name = String(argument.drop { $0 == "-" })
            if let separator = name.firstIndex(of: "=") {
                options[String(name[name.startIndex..<separator])] =
                    String(name[name.index(after: separator)...])
                continue
            }

            if Self.knownFlags.contains(name) {
                flags.insert(name)
                continue
            }

            guard index < arguments.count, !arguments[index].hasPrefix("--") else {
                throw UsageError("--\(name) needs a value")
            }
            options[name] = arguments[index]
            index += 1
        }
    }

    /// The command word, or `nil` when the line was empty.
    var command: String? { positional.first }

    /// A positional word after the command, by position. `operand(0)` is the
    /// first word after the command.
    func operand(_ position: Int) -> String? {
        let index = position + 1
        return positional.indices.contains(index) ? positional[index] : nil
    }

    /// A required positional word.
    func requireOperand(_ position: Int, _ name: String) throws -> String {
        guard let value = operand(position) else {
            throw UsageError("missing \(name)")
        }
        return value
    }

    mutating func string(_ name: String) -> String? {
        consumed.insert(name)
        return options[name]
    }

    mutating func requireString(_ name: String) throws -> String {
        guard let value = string(name) else {
            throw UsageError("--\(name) is required")
        }
        return value
    }

    mutating func int(_ name: String) throws -> Int? {
        guard let raw = string(name) else { return nil }
        guard let value = Int(raw) else {
            throw UsageError("--\(name) needs a whole number, got \(raw)")
        }
        return value
    }

    mutating func int64(_ name: String) throws -> Int64? {
        guard let raw = string(name) else { return nil }
        guard let value = Int64(raw) else {
            throw UsageError("--\(name) needs a whole number, got \(raw)")
        }
        return value
    }

    mutating func int32(_ name: String) throws -> Int32? {
        guard let raw = string(name) else { return nil }
        guard let value = Int32(raw) else {
            throw UsageError("--\(name) needs a whole number, got \(raw)")
        }
        return value
    }

    mutating func flag(_ name: String) -> Bool {
        consumed.insert(name)
        return flags.contains(name)
    }

    var wantsHelp: Bool { flags.contains("help") || flags.contains("h") }
    var wantsJSON: Bool { flags.contains("json") }

    /// Throws if an option or flag was passed that the command never read.
    ///
    /// Called at the end of each command, because a typo in an option name is
    /// otherwise invisible: the command runs with a default and reports
    /// success, which is worse than refusing to run.
    func rejectUnknownOptions() throws {
        // `json` and `help` are global and handled outside the commands.
        let global: Set<String> = [
            "json", "help", "h", "cluster", "bootstrap", "timeout", "no-keychain"
        ]
        let unknown = (Set(options.keys).union(flags))
            .subtracting(consumed)
            .subtracting(global)
            .sorted()

        guard unknown.isEmpty else {
            throw UsageError(
                "unrecognised option\(unknown.count == 1 ? "" : "s"): "
                    + unknown.map { "--\($0)" }.joined(separator: ", ")
            )
        }
    }
}
