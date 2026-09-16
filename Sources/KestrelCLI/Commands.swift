import Foundation
import KestrelKit

/// The commands, each one a function over a parsed command line.
///
/// Every command calls the same KestrelKit types the app does, so the two
/// cannot disagree about what a topic list or a group's lag is.
enum Commands {
    // MARK: clusters

    /// Lists the saved cluster profiles.
    ///
    /// Needs no broker and no cluster option: it reads the same
    /// `clusters.json` the app writes, which is how `--cluster` names are
    /// discovered in the first place.
    static func clusters(_ arguments: inout Arguments) throws {
        try arguments.rejectUnknownOptions()
        let output = Output(wantsJSON: arguments.wantsJSON)
        let profiles = try ClusterProfileRepository().load()

        output.table(
            columns: ["Name", "Bootstrap Servers", "Security", "Compression", "Registry", "Connect"],
            rows: profiles
                .sorted { $0.name < $1.name }
                .map { profile in
                    [
                        profile.name,
                        profile.bootstrapServers,
                        profile.securityProtocol.rawValue,
                        // Listed because it silently changes what a produce
                        // puts on the wire, and nothing else in the terminal
                        // would say the profile had asked for it.
                        profile.compression.rawValue,
                        profile.schemaRegistry.isConfigured ? profile.schemaRegistry.url : "—",
                        profile.connect.isConfigured ? profile.connect.url : "—"
                    ]
                }
        )
    }

    // MARK: topics

    /// Lists topics, describes one, or creates and deletes them.
    ///
    /// `create` and `delete` are actions under `topics` rather than commands of
    /// their own, which does mean a topic named "create" has to be described
    /// through `--name`. That is the cheaper of the two ambiguities.
    static func topics(_ arguments: inout Arguments, _ context: Context) async throws {
        let first = arguments.operand(0)

        if first == "create" || first == "delete" {
            try await manageTopic(action: first!, &arguments, context)
            return
        }

        let name = arguments.string("name") ?? first
        let showsInternal = arguments.flag("internal")
        try arguments.rejectUnknownOptions()

        let client = try context.client()
        let metadata = try await client.metadata(timeout: context.timeout)

        guard let name else {
            // Internal topics are hidden by default, matching the app: a list
            // led by `__consumer_offsets` is not what was asked for.
            let topics = metadata.topics
                .filter { showsInternal || !$0.isInternal }
                .sorted { $0.name < $1.name }

            context.output.table(
                columns: ["Topic", "Partitions", "Replication", "Internal"],
                rows: topics.map {
                    [
                        $0.name,
                        "\($0.partitionCount)",
                        "\($0.partitions.first?.replicas.count ?? 0)",
                        $0.isInternal ? "yes" : "no"
                    ]
                }
            )
            return
        }

        let topic = try await requireTopic(name, context)

        context.output.table(
            columns: ["Partition", "Leader", "Replicas", "In Sync"],
            rows: topic.partitions.map {
                [
                    "\($0.id)",
                    "\($0.leader)",
                    $0.replicas.map(String.init).joined(separator: ","),
                    $0.inSyncReplicas.map(String.init).joined(separator: ",")
                ]
            }
        )
    }

    /// Creates or deletes a topic.
    private static func manageTopic(
        action: String,
        _ arguments: inout Arguments,
        _ context: Context
    ) async throws {
        let name = try arguments.operand(1) ?? arguments.requireString("name")
        let partitions = try arguments.int32("partitions") ?? 1
        let replication = try arguments.int32("replication") ?? 1
        let configList = arguments.string("config")
        try arguments.rejectUnknownOptions()

        let client = try context.client()

        guard action == "create" else {
            try await client.deleteTopic(name: name, timeout: context.timeout)
            context.output.note("deleted \(name)")
            return
        }

        var configs: [String: String] = [:]
        for pair in configList?.split(separator: ",") ?? [] {
            guard let separator = pair.firstIndex(of: "=") else {
                throw UsageError("--config takes key=value pairs, got \(pair)")
            }
            configs[String(pair[pair.startIndex..<separator])] =
                String(pair[pair.index(after: separator)...])
        }

        // `false` means the broker already had it, which is not a failure: it
        // leaves a script that creates before importing idempotent.
        let created = try await client.createTopic(
            name: name,
            partitions: partitions,
            replicationFactor: replication,
            configs: configs,
            timeout: context.timeout
        )
        context.output.note(
            created
                ? "created \(name) with \(partitions) partition(s), replication \(replication)"
                : "\(name) already exists, left as it is"
        )
    }

    // MARK: brokers

    /// Lists the brokers in the cluster.
    static func brokers(_ arguments: inout Arguments, _ context: Context) async throws {
        try arguments.rejectUnknownOptions()

        let client = try context.client()
        let metadata = try await client.metadata(timeout: context.timeout)

        context.output.table(
            columns: ["ID", "Host", "Port"],
            rows: metadata.brokers
                .sorted { $0.id < $1.id }
                .map { ["\($0.id)", $0.host, "\($0.port)"] }
        )
    }

    // MARK: consume

    /// Reads a page of records from one partition.
    static func consume(_ arguments: inout Arguments, _ context: Context) async throws {
        let topic = try arguments.requireOperand(0, "a topic name")
        let partition = try arguments.int32("partition") ?? 0
        let limit = try arguments.int("limit") ?? 10
        let offset = try arguments.int64("offset")
        let tail = try arguments.int("tail")
        let showsHeaders = !arguments.flag("no-headers")
        try arguments.rejectUnknownOptions()

        let start: StartPosition
        switch (offset, tail) {
        case (let offset?, nil): start = .offset(offset)
        case (nil, let tail?): start = .latest(count: Int64(tail))
        case (nil, nil): start = .earliest
        case (_?, _?): throw UsageError("pass either --offset or --tail, not both")
        }

        _ = try await requireTopic(topic, context, partition: partition)

        let consumer = try context.consumer()
        let page = try await consumer.fetch(
            topic: topic,
            partition: partition,
            from: start,
            limit: limit,
            timeout: context.timeout
        )

        if arguments.wantsJSON {
            // Envelopes, the same shape the app saves and `import` reads, so a
            // consume can be piped straight back into a produce.
            for record in page.records {
                let envelope = RecordEnvelope(record: record, topic: topic)
                let data = try JSONEncoder().encode(envelope)
                context.output.line(String(data: data, encoding: .utf8) ?? "")
            }
            return
        }

        context.output.note(
            "\(page.records.count) record(s) from offset \(page.startOffset) · "
                + "partition holds \(page.lowWatermark)…\(page.highWatermark)"
        )
        for record in page.records {
            let time = record.timestamp?.formatted(.iso8601) ?? "—"
            context.output.line("--- offset \(record.offset) · \(time)")
            context.output.line("key:   \(PayloadText.describe(record.key))")
            context.output.line("value: \(await avroOrBytes(record.value, context))")
            if showsHeaders, !record.headers.isEmpty {
                for header in record.headers {
                    context.output.line("header \(header.name): \(header.displayValue)")
                }
            }
        }
    }

    // MARK: produce

    /// Sends one record.
    static func produce(_ arguments: inout Arguments, _ context: Context) async throws {
        let topic = try arguments.requireOperand(0, "a topic name")
        let key = arguments.string("key")
        let value = arguments.string("value")
        let partition = try arguments.int32("partition")
        let isTombstone = arguments.flag("tombstone")
        let headerList = arguments.string("header")
        let subject = arguments.string("subject")
        try arguments.rejectUnknownOptions()

        if value == nil, !isTombstone {
            throw UsageError("--value is required (or --tombstone for a null value)")
        }
        if value != nil, isTombstone {
            throw UsageError("--tombstone sends a null value, so --value cannot be given too")
        }

        var body: Data?
        if let value {
            // With --subject the value is JSON to be encoded as Avro against
            // that subject's newest schema, which is what the app's produce
            // sheet does.
            body = if let subject {
                try await AvroPayload.encode(
                    json: value,
                    subject: subject,
                    registry: try context.registry()
                )
            } else {
                Data(value.utf8)
            }
        }

        let headers = try (headerList?.split(separator: ",") ?? []).map { pair -> RecordHeader in
            guard let separator = pair.firstIndex(of: "=") else {
                throw UsageError("--header takes name=value pairs, got \(pair)")
            }
            return RecordHeader(
                name: String(pair[pair.startIndex..<separator]),
                value: Data(String(pair[pair.index(after: separator)...]).utf8)
            )
        }

        _ = try await requireTopic(topic, context, partition: partition)

        let client = try context.client()
        let report = try await client.produce(
            topic: topic,
            partition: partition,
            key: key.map { Data($0.utf8) },
            value: body,
            headers: headers,
            timeout: context.timeout
        )

        context.output.fields([
            ("Topic", topic),
            ("Partition", "\(report.partition)"),
            ("Offset", "\(report.offset)")
        ])
    }

    // MARK: groups

    /// Lists consumer groups, or describes one with its per-partition lag.
    static func groups(_ arguments: inout Arguments, _ context: Context) async throws {
        let name = arguments.operand(0)
        try arguments.rejectUnknownOptions()

        let client = try context.client()

        guard let name else {
            let groups = try await client.consumerGroups(timeout: context.timeout)
            context.output.table(
                columns: ["Group", "State", "Members", "Topics"],
                rows: groups
                    .sorted { $0.id < $1.id }
                    .map {
                        [
                            $0.id,
                            $0.state,
                            "\($0.members.count)",
                            $0.subscribedTopics.sorted().joined(separator: ",")
                        ]
                    }
            )
            return
        }

        let rows = try await client.groupOffsets(group: name, timeout: context.timeout)
        guard !rows.isEmpty else {
            throw CLIFailure("group \(name) has no committed offsets, or does not exist")
        }

        context.output.table(
            columns: ["Topic", "Partition", "Committed", "Log End", "Lag"],
            rows: rows.map {
                [
                    $0.topic,
                    "\($0.partition)",
                    $0.committed.map(String.init) ?? "—",
                    "\($0.logEnd)",
                    $0.lag.map(String.init) ?? "—"
                ]
            }
        )

        if !arguments.wantsJSON {
            let lag = rows.compactMap(\.lag).reduce(0, +)
            context.output.note("total lag: \(lag)")
        }
    }

    // MARK: schema

    /// Lists registry subjects, or fetches one subject's newest schema.
    static func schema(_ arguments: inout Arguments, _ context: Context) async throws {
        let subject = arguments.operand(0)
        let id = try arguments.int32("id")
        try arguments.rejectUnknownOptions()

        let registry = try context.registry()

        if let id {
            let schema = try await registry.schema(id: id)
            context.output.line(schema.text)
            return
        }

        guard let subject else {
            let subjects = try await registry.subjects()
            context.output.table(columns: ["Subject"], rows: subjects.map { [$0] })
            return
        }

        let schema = try await registry.latestSchema(subject: subject)
        if arguments.wantsJSON {
            context.output.line(schema.text)
        } else {
            context.output.note("subject \(subject) version \(schema.version.map(String.init) ?? "?") id \(schema.id)")
            context.output.line(schema.text)
        }
    }

    // MARK: connect

    /// Lists connectors, or runs an action on one.
    static func connect(_ arguments: inout Arguments, _ context: Context) async throws {
        let action = arguments.operand(0) ?? "list"
        let name = arguments.operand(1)
        try arguments.rejectUnknownOptions()

        let client = try context.connect()

        switch action {
        case "list":
            let connectors = try await client.connectors()
            context.output.table(
                columns: ["Connector", "Type", "State", "Tasks", "Class"],
                rows: connectors.map { connector in
                    let running = connector.tasks.filter { $0.state == .running }.count
                    return [
                        connector.name,
                        connector.kind.rawValue,
                        connector.state.label,
                        "\(running)/\(connector.tasks.count)",
                        connector.connectorClass ?? "—"
                    ]
                }
            )

        case "status":
            guard let name else { throw UsageError("connect status needs a connector name") }
            let connector = try await client.connector(named: name)
            context.output.fields([
                ("Connector", connector.name),
                ("Type", connector.kind.rawValue),
                ("State", connector.state.label),
                ("Worker", connector.workerID),
                ("Tasks", "\(connector.tasks.count)")
            ])
            guard !arguments.wantsJSON else { return }
            for task in connector.tasks {
                context.output.line("task \(task.id): \(task.state.label) on \(task.workerID)")
                if let trace = task.trace?.split(separator: "\n").first {
                    context.output.line("  \(trace)")
                }
            }

        case "config":
            guard let name else { throw UsageError("connect config needs a connector name") }
            let connector = try await client.connector(named: name)
            context.output.table(
                columns: ["Key", "Value"],
                rows: connector.config.keys.sorted().map { [$0, connector.config[$0] ?? ""] }
            )

        case "pause", "resume", "restart":
            guard let name else { throw UsageError("connect \(action) needs a connector name") }
            switch action {
            case "pause": try await client.pause(name)
            case "resume": try await client.resume(name)
            default: try await client.restart(name, includeTasks: true)
            }
            // Connect accepts these asynchronously, so the state afterwards is
            // not reported here; it would usually still be the old one.
            context.output.note("\(action) accepted for \(name)")

        default:
            throw UsageError(
                "unknown connect action \(action). Use list, status, config, pause, resume or restart."
            )
        }
    }

    // MARK: import / export / generate / find

    /// Produces the records in a file, the same reader the app's import uses.
    static func importRecords(_ arguments: inout Arguments, _ context: Context) async throws {
        let path = try arguments.requireOperand(0, "a file to import")
        let topic = try arguments.requireString("topic")
        try arguments.rejectUnknownOptions()

        _ = try await requireTopic(topic, context)

        let plan = try RecordImporter.plan(fileURL: URL(fileURLWithPath: path))
        context.output.note(
            "\(plan.records.count) record(s) to import (\(plan.kind.rawValue)), "
                + "\(plan.problems.count) unreadable line(s), \(plan.blankLines) blank"
        )
        for problem in plan.problems {
            printError("line \(problem.line): \(problem.message)")
        }
        guard !plan.isEmpty else {
            throw CLIFailure("nothing to import from \(path)")
        }

        let client = try context.client()
        let outcome = await RecordImporter.run(plan: plan) { record in
            try await client.produce(
                topic: topic,
                partition: record.partition,
                key: record.key,
                value: record.value,
                headers: record.headers,
                timeout: context.timeout
            )
        }

        for problem in outcome.produceProblems {
            printError("line \(problem.line): \(problem.message)")
        }
        context.output.fields([
            ("Topic", topic),
            ("Produced", "\(outcome.produced)"),
            ("Failed", "\(outcome.produceProblems.count)")
        ])

        if outcome.produced < plan.records.count {
            throw CLIFailure("\(plan.records.count - outcome.produced) record(s) were not produced")
        }
    }

    /// Writes a topic's records to a JSON Lines file, or into another topic.
    static func export(_ arguments: inout Arguments, _ context: Context) async throws {
        let topic = try arguments.requireOperand(0, "a topic name")
        let file = arguments.string("file")
        let toTopic = arguments.string("to-topic")
        let partition = try arguments.int32("partition")
        let start = try arguments.int64("from")
        let end = try arguments.int64("to")
        try arguments.rejectUnknownOptions()

        guard file != nil || toTopic != nil else {
            throw UsageError("pass --file <path> or --to-topic <name>")
        }
        if file != nil, toTopic != nil {
            throw UsageError("pass either --file or --to-topic, not both")
        }

        _ = try await requireTopic(topic, context, partition: partition)
        if let toTopic {
            _ = try await requireTopic(toTopic, context)
        }

        let consumer = try context.consumer()
        let request = ExportRequest(
            topic: topic,
            partitions: partition.map { [$0] } ?? [],
            startOffset: start,
            endOffset: end
        )

        let sink: ExportSink
        if let file {
            sink = try JSONLFileSink(url: URL(fileURLWithPath: file))
        } else {
            let client = try context.client()
            let destination = toTopic!
            sink = TopicSink { record in
                try await client.produce(
                    topic: destination,
                    partition: nil,
                    key: record.key,
                    value: record.value,
                    headers: record.headers,
                    timeout: context.timeout
                )
            }
        }

        let outcome = try await RecordExporter.export(
            request: request,
            consumer: consumer,
            sink: sink
        )

        context.output.fields([
            ("Topic", topic),
            ("Destination", file ?? toTopic ?? ""),
            ("Records", "\(outcome.records)")
        ])
    }

    /// Generates records from key and value templates.
    static func generate(_ arguments: inout Arguments, _ context: Context) async throws {
        let topic = try arguments.requireOperand(0, "a topic name")
        let count = try arguments.int("count") ?? 10
        let keyTemplate = arguments.string("key") ?? ""
        let valueTemplate = arguments.string("value") ?? #"{"index":{{index}},"id":"{{uuid}}"}"#
        let partition = try arguments.int32("partition")
        let seed = try arguments.int("seed").map { UInt64($0) }
        try arguments.rejectUnknownOptions()

        _ = try await requireTopic(topic, context, partition: partition)

        let unknown = TemplateExpander.unknownPlaceholders(in: keyTemplate + valueTemplate)
        for placeholder in unknown {
            printError("unknown placeholder {{\(placeholder)}}, sent as written")
        }

        let request = GenerateRequest(
            topic: topic,
            count: count,
            keyTemplate: keyTemplate,
            valueTemplate: valueTemplate,
            partition: partition,
            seed: seed
        )

        let generator = RecordGenerator(client: try context.client())
        let outcome = await generator.generate(request: request)

        for (index, message) in outcome.failures.sorted(by: { $0.key < $1.key }) {
            printError("record \(index): \(message)")
        }
        context.output.fields([
            ("Topic", topic),
            ("Produced", "\(outcome.produced)"),
            ("Failed", "\(outcome.failures.count)")
        ])

        if outcome.produced < count {
            throw CLIFailure("\(count - outcome.produced) record(s) were not produced")
        }
    }

    /// Scans topics for a string or a regex.
    static func find(_ arguments: inout Arguments, _ context: Context) async throws {
        let text = try arguments.requireOperand(0, "something to search for")
        let topicList = arguments.string("topic")
        let isRegex = arguments.flag("regex")
        let isCaseSensitive = arguments.flag("case-sensitive")
        let keysOnly = arguments.flag("keys-only")
        let valuesOnly = arguments.flag("values-only")
        let limit = try arguments.int("limit") ?? 100
        try arguments.rejectUnknownOptions()

        if keysOnly, valuesOnly {
            throw UsageError("pass either --keys-only or --values-only, not both")
        }

        let client = try context.client()
        let metadata = try await client.metadata(timeout: context.timeout)
        let wanted = topicList?.split(separator: ",").map(String.init)

        let scopes = metadata.topics
            .filter { topic in
                guard !topic.isInternal else { return false }
                guard let wanted else { return true }
                return wanted.contains(topic.name)
            }
            .sorted { $0.name < $1.name }
            .map { SearchScope(topic: $0.name, partitions: $0.partitions.map(\.id)) }

        if let wanted {
            let missing = wanted.filter { name in !scopes.contains { $0.topic == name } }
            for name in missing {
                printError("no topic named \(name), skipped")
            }
        }

        let query = SearchQuery(
            text: text,
            isRegex: isRegex,
            searchesKeys: !valuesOnly,
            searchesValues: !keysOnly,
            isCaseSensitive: isCaseSensitive,
            maxHits: limit
        )

        let searcher = RecordSearcher(consumer: try context.consumer())
        let outcome = try await searcher.scan(query: query, scopes: scopes)

        context.output.table(
            columns: ["Topic", "Partition", "Offset", "Field", "Match"],
            rows: outcome.hits.map {
                [$0.topic, "\($0.partition)", "\($0.offset)", $0.field.rawValue, $0.excerpt]
            }
        )
        context.output.note(
            "\(outcome.hits.count) hit(s) in \(outcome.scanned) record(s)"
                + (outcome.reachedLimit ? " · stopped at the limit of \(limit)" : "")
        )
    }
}

/// Renders a value, decoding it first when it is an Avro payload.
///
/// The app's detail pane decodes Confluent-framed values, so the CLI does too:
/// a tool that prints base64 where the window shows JSON is not the same tool.
/// A payload that is not Avro, or a registry that cannot be reached, falls back
/// to the bytes with the reason appended rather than failing the whole read.
func avroOrBytes(_ value: Data?, _ context: Context) async -> String {
    guard let value, AvroPayload.isFramed(value) else {
        return PayloadText.describe(value)
    }

    do {
        guard let decoded = try await AvroPayload.decode(value, registry: try context.registry())
        else {
            return PayloadText.describe(value)
        }
        return "\(decoded.json)\n       (Avro, schema id \(decoded.schemaID))"
    } catch {
        let reason = (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
        return "\(PayloadText.describe(value))\n       (looks like Avro but: \(reason))"
    }
}

/// Checks a topic exists before a command tries to use it.
///
/// Worth the extra metadata call: with auto-create off, producing to a topic
/// that is not there fails as a request timeout or "unknown partition", which
/// names neither the topic nor the real problem.
func requireTopic(_ name: String, _ context: Context, partition: Int32? = nil) async throws -> TopicInfo {
    let client = try context.client()
    let metadata = try await client.metadata(timeout: context.timeout)

    guard let topic = metadata.topics.first(where: { $0.name == name }) else {
        // Ranked by shared prefix, and only suggested when the overlap is at
        // least half the name: a fixed-length prefix offers "kestrel-connect-
        // configs" for "kestrel.cli.nope", which is noise dressed as help.
        let candidates = metadata.topics
            .map { (name: $0.name, shared: $0.name.commonPrefix(with: name).count) }
            .filter { $0.shared * 2 >= name.count }
            .sorted { ($0.shared, $1.name) > ($1.shared, $0.name) }
            .prefix(3)
            .map(\.name)
        let hint = candidates.isEmpty ? "" : ". Did you mean \(candidates.joined(separator: ", "))?"
        throw CLIFailure("no topic named \(name) on \(context.profile.bootstrapServers)\(hint)")
    }

    if let partition, !topic.partitions.contains(where: { $0.id == partition }) {
        throw CLIFailure(
            "topic \(name) has no partition \(partition); it has 0…\(topic.partitionCount - 1)"
        )
    }
    return topic
}

/// A command failed for a reason that is not a usage mistake.
struct CLIFailure: Error, CustomStringConvertible {
    let description: String

    init(_ description: String) {
        self.description = description
    }
}
