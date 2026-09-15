import Foundation

/// Fills in the placeholders of a record template.
///
/// Templates are plain text with `{{...}}` placeholders, so a value can be
/// anything from a bare number to a JSON document with generated fields:
///
/// ```
/// {"id":"{{uuid}}","n":{{index}},"note":"{{lorem:4}}"}
/// ```
///
/// Anything that is not a recognised placeholder is left exactly as written,
/// including an unrecognised `{{...}}`, so a typo shows up in the output
/// instead of quietly producing an empty field. ``unknownPlaceholders(in:)``
/// finds those before a run starts.
public struct TemplateExpander {
    /// Placeholders this build understands, with what they produce.
    public static let documentation: [(name: String, describes: String)] = [
        ("{{index}}", "the record's number, starting at 0"),
        ("{{uuid}}", "a random UUID"),
        ("{{timestamp}}", "the current time, ISO 8601"),
        ("{{millis}}", "the current time in milliseconds since the epoch"),
        ("{{lorem}}", "five random words"),
        ("{{lorem:n}}", "n random words"),
        ("{{int:a-b}}", "a random whole number from a to b"),
        ("{{choice:x|y|z}}", "one of the listed options"),
    ]

    /// Words the `lorem` placeholder draws from.
    private static let words = [
        "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
        "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi",
    ]

    private var generator: any RandomNumberGenerator

    /// - Parameter generator: source of randomness. Pass a seeded generator to
    ///   get the same output twice, which is what the checks rely on.
    public init(generator: any RandomNumberGenerator = SystemRandomNumberGenerator()) {
        self.generator = generator
    }

    /// Expands one template.
    ///
    /// - Parameters:
    ///   - template: text with `{{...}}` placeholders.
    ///   - index: the record's number, for `{{index}}`.
    ///   - now: the time `{{timestamp}}` and `{{millis}}` report.
    /// - Returns: the text with every recognised placeholder replaced.
    public mutating func expand(
        _ template: String,
        index: Int,
        now: Date = Date()
    ) -> String {
        var output = ""
        var rest = Substring(template)

        while let open = rest.range(of: "{{") {
            output += rest[rest.startIndex..<open.lowerBound]
            let afterOpen = rest[open.upperBound...]

            guard let close = afterOpen.range(of: "}}") else {
                // An unterminated placeholder is just text.
                output += rest[open.lowerBound...]
                return output
            }

            let name = String(afterOpen[afterOpen.startIndex..<close.lowerBound])
            output += value(for: name, index: index, now: now)
                ?? "{{\(name)}}"
            rest = afterOpen[close.upperBound...]
        }

        return output + rest
    }

    /// The placeholders in a template that this build does not understand.
    ///
    /// - Returns: their names, in the order they appear, without duplicates.
    public static func unknownPlaceholders(in template: String) -> [String] {
        var expander = TemplateExpander(generator: SeededGenerator(seed: 0))
        var unknown: [String] = []
        var rest = Substring(template)

        while let open = rest.range(of: "{{") {
            let afterOpen = rest[open.upperBound...]
            guard let close = afterOpen.range(of: "}}") else { break }
            let name = String(afterOpen[afterOpen.startIndex..<close.lowerBound])
            if expander.value(for: name, index: 0, now: Date()) == nil, !unknown.contains(name) {
                unknown.append(name)
            }
            rest = afterOpen[close.upperBound...]
        }

        return unknown
    }

    /// Resolves one placeholder, or `nil` if it is not recognised.
    private mutating func value(for name: String, index: Int, now: Date) -> String? {
        switch name {
        case "index":
            return String(index)
        case "uuid":
            return uuid()
        case "timestamp":
            return now.formatted(.iso8601)
        case "millis":
            return String(Int64((now.timeIntervalSince1970 * 1000).rounded()))
        case "lorem":
            return lorem(count: 5)
        default:
            break
        }

        if let argument = name.after("lorem:") {
            guard let count = Int(argument), count > 0, count <= 500 else { return nil }
            return lorem(count: count)
        }

        if let argument = name.after("int:") {
            let bounds = argument.split(separator: "-", maxSplits: 1).compactMap { Int64($0) }
            guard bounds.count == 2, bounds[0] <= bounds[1] else { return nil }
            return String(Int64.random(in: bounds[0]...bounds[1], using: &generator))
        }

        if let argument = name.after("choice:") {
            let options = argument.split(separator: "|").map(String.init)
            guard !options.isEmpty else { return nil }
            return options[Int.random(in: 0..<options.count, using: &generator)]
        }

        return nil
    }

    /// A UUID drawn from `generator`, so a seeded run repeats exactly.
    ///
    /// `UUID()` ignores the generator, which would make seeded output differ
    /// from run to run.
    private mutating func uuid() -> String {
        var bytes = (0..<16).map { _ in UInt8.random(in: 0...255, using: &generator) }
        // Version 4, variant 1, as a random UUID is expected to report.
        bytes[6] = (bytes[6] & 0x0f) | 0x40
        bytes[8] = (bytes[8] & 0x3f) | 0x80

        let hex = bytes.map { String(format: "%02x", $0) }.joined()
        let groups = [0..<8, 8..<12, 12..<16, 16..<20, 20..<32].map { range in
            String(hex[hex.index(hex.startIndex, offsetBy: range.lowerBound)..<hex.index(hex.startIndex, offsetBy: range.upperBound)])
        }
        return groups.joined(separator: "-")
    }

    private mutating func lorem(count: Int) -> String {
        (0..<count)
            .map { _ in Self.words[Int.random(in: 0..<Self.words.count, using: &generator)] }
            .joined(separator: " ")
    }
}

private extension String {
    /// The text after `prefix`, or `nil` if it does not start with it.
    func after(_ prefix: String) -> String? {
        hasPrefix(prefix) ? String(dropFirst(prefix.count)) : nil
    }
}

/// A reproducible random source, for checks and for previewing a template.
///
/// SplitMix64: small, well distributed, and identical across platforms and
/// releases, which `SystemRandomNumberGenerator` deliberately is not.
public struct SeededGenerator: RandomNumberGenerator {
    private var state: UInt64

    public init(seed: UInt64) {
        self.state = seed
    }

    public mutating func next() -> UInt64 {
        state = state &+ 0x9e37_79b9_7f4a_7c15
        var z = state
        z = (z ^ (z >> 30)) &* 0xbf58_476d_1ce4_e5b9
        z = (z ^ (z >> 27)) &* 0x94d0_49bb_1331_11eb
        return z ^ (z >> 31)
    }
}
