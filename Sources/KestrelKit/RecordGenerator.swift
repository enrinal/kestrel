import Foundation

/// What to generate.
public struct GenerateRequest: Sendable {
    public let topic: String
    public let count: Int
    /// Template for the key. Empty produces a keyless record.
    public let keyTemplate: String
    /// Template for the value.
    public let valueTemplate: String
    /// Partition to send every record to. `nil` lets the broker decide.
    public let partition: Int32?
    /// Seed for reproducible output. `nil` uses system randomness.
    public let seed: UInt64?

    public init(
        topic: String,
        count: Int,
        keyTemplate: String = "",
        valueTemplate: String,
        partition: Int32? = nil,
        seed: UInt64? = nil
    ) {
        self.topic = topic
        self.count = count
        self.keyTemplate = keyTemplate
        self.valueTemplate = valueTemplate
        self.partition = partition
        self.seed = seed
    }
}

/// What a generation run produced.
public struct GenerateOutcome: Sendable, Equatable {
    public let produced: Int
    /// Records the broker refused, by their index in the run.
    public let failures: [Int: String]
    /// Offsets assigned, in the order produced.
    public let offsets: [Int64]

    public init(produced: Int, failures: [Int: String], offsets: [Int64]) {
        self.produced = produced
        self.failures = failures
        self.offsets = offsets
    }
}

/// Fills a topic with records built from templates.
///
/// An actor, so expanding templates and waiting on the broker both happen off
/// the main thread: generating tens of thousands of records must not make the
/// window stop drawing.
public actor RecordGenerator {
    private let client: KafkaClient

    public init(client: KafkaClient) {
        self.client = client
    }

    /// Previews what a run would produce, without sending anything.
    ///
    /// - Parameters:
    ///   - request: the run to preview.
    ///   - limit: how many records to render.
    /// - Returns: the key and value of the first `limit` records.
    public static func preview(
        request: GenerateRequest,
        limit: Int = 3
    ) -> [(key: String?, value: String)] {
        var expander = TemplateExpander(generator: SeededGenerator(seed: request.seed ?? 0))
        let now = Date()

        return (0..<min(limit, request.count)).map { index in
            let key = request.keyTemplate.isEmpty
                ? nil
                : expander.expand(request.keyTemplate, index: index, now: now)
            return (key, expander.expand(request.valueTemplate, index: index, now: now))
        }
    }

    /// Generates and produces every record.
    ///
    /// A record the broker refuses is recorded and the run carries on, so a
    /// single rejection does not waste the rest of a long run.
    ///
    /// - Parameters:
    ///   - request: what to generate.
    ///   - progress: called with the number produced so far, at intervals
    ///     rather than for every record, so a long run does not spend its time
    ///     updating a progress bar.
    /// - Returns: how many records the broker took, and any that it refused.
    public func generate(
        request: GenerateRequest,
        progress: (@Sendable (Int, Int) -> Void)? = nil
    ) async -> GenerateOutcome {
        var expander = TemplateExpander(
            generator: request.seed.map { SeededGenerator(seed: $0) as any RandomNumberGenerator }
                ?? SystemRandomNumberGenerator()
        )

        var offsets: [Int64] = []
        var failures: [Int: String] = [:]
        let now = Date()
        // Report at most a hundred times over the whole run.
        let step = max(1, request.count / 100)

        for index in 0..<request.count {
            let key = request.keyTemplate.isEmpty
                ? nil
                : Data(expander.expand(request.keyTemplate, index: index, now: now).utf8)
            let value = Data(expander.expand(request.valueTemplate, index: index, now: now).utf8)

            do {
                let report = try await client.produce(
                    topic: request.topic,
                    partition: request.partition,
                    key: key,
                    value: value
                )
                offsets.append(report.offset)
            } catch {
                failures[index] = (error as? LocalizedError)?.errorDescription
                    ?? error.localizedDescription
            }

            if (index + 1) % step == 0 || index + 1 == request.count {
                progress?(offsets.count, request.count)
            }
        }

        return GenerateOutcome(produced: offsets.count, failures: failures, offsets: offsets)
    }
}
