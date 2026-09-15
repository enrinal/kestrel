import KestrelKit
import SwiftUI

struct RootView: View {
    @Environment(ClusterStore.self) private var store

    var body: some View {
        NavigationSplitView {
            SidebarView()
        } detail: {
            DetailView()
        }
        .navigationTitle("Kestrel")
        .modifier(EditorSheets())
        .modifier(TopicManagementSheets())
        .modifier(Notices())
        .task {
            guard Snapshot.isRequested else { return }
            for action in Snapshot.requestedActions {
                switch action {
                case "select-first":
                    store.selection = store.clusters.first.map { SidebarItem.cluster($0.id) }
                case "connect":
                    if let id = store.selection?.clusterID { await store.connect(id: id) }
                case "expand-groups":
                    if let id = store.selection?.clusterID {
                        store.expand(id: id, branches: [.groupsFolder(id), .brokersFolder(id)])
                    }
                case "expand-all":
                    if let id = store.selection?.clusterID { store.expandAll(id: id) }
                case "select-broker":
                    if let id = store.selection?.clusterID,
                       let broker = store.connection(for: id)?.brokers.first {
                        store.selection = .broker(id, broker.id)
                    }
                case "select-topic":
                    // KESTREL_SNAPSHOT_TOPIC picks a specific topic; without it
                    // the first topic that has partitions is used.
                    if let id = store.selection?.clusterID {
                        let wanted = ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_TOPIC"]
                        let topics = store.visibleTopics(for: id)
                        let topic = wanted.flatMap { name in topics.first { $0.name == name } }
                            ?? topics.first { !$0.partitions.isEmpty }
                        if let topic { store.selection = .topic(id, topic.name) }
                    }
                case "load-configs":
                    switch store.selection {
                    case .topic(let clusterID, let name):
                        await store.connection(for: clusterID)?.loadConfigs(for: .topic(name))
                    case .broker(let clusterID, let brokerID):
                        await store.connection(for: clusterID)?.loadConfigs(for: .broker(brokerID))
                    default:
                        break
                    }
                case "load-messages":
                    if case .topic(let clusterID, let name) = store.selection,
                       let connection = store.connection(for: clusterID),
                       let topic = connection.topic(named: name) {
                        await connection.loadRecords(
                            topic: name,
                            partition: topic.partitions.first?.id ?? 0,
                            from: .earliest,
                            limit: 10
                        )
                    }
                case "load-messages-tail":
                    if case .topic(let clusterID, let name) = store.selection,
                       let connection = store.connection(for: clusterID),
                       let topic = connection.topic(named: name) {
                        await connection.loadRecords(
                            topic: name,
                            partition: topic.partitions.first?.id ?? 0,
                            from: .latest(count: 3),
                            limit: 10
                        )
                    }
                case "produce":
                    // Writes a record to the selected topic. Point this at a
                    // scratch topic only; KESTREL_SNAPSHOT_TOPIC selects it.
                    if case .topic(let clusterID, let name) = store.selection,
                       let connection = store.connection(for: clusterID) {
                        do {
                            let report = try await connection.produce(
                                topic: name,
                                partition: nil,
                                key: Data("snapshot-key".utf8),
                                value: Data(#"{"source":"snapshot"}"#.utf8),
                                headers: [RecordHeader(name: "origin", value: Data("snapshot".utf8))]
                            )
                            store.toast = """
                                Produced to \(name) partition \(report.partition), \
                                offset \(report.offset)
                                """
                            print("DUMP_PRODUCED partition=\(report.partition) offset=\(report.offset)")
                        } catch {
                            print("DUMP_PRODUCE_FAILED \(ClusterConnection.message(for: error))")
                        }
                    }
                case "open-produce":
                    if case .topic(let clusterID, let name) = store.selection {
                        store.produceTarget = ProduceTarget(clusterID: clusterID, topic: name)
                    }
                case "select-group":
                    if let id = store.selection?.clusterID {
                        let wanted = ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_GROUP"]
                        let groups = store.connection(for: id)?.groups ?? []
                        let group = wanted.flatMap { name in groups.first { $0.id == name } }
                            ?? groups.first
                        if let group { store.selection = .group(id, group.id) }
                    }
                case "load-offsets":
                    if case .group(let clusterID, let groupID) = store.selection {
                        await store.connection(for: clusterID)?.loadOffsets(forGroup: groupID)
                    }
                case "create-smoke-topic":
                    if let clusterID = store.selection?.clusterID,
                       let connection = store.connection(for: clusterID) {
                        do {
                            let created = try await connection.createTopic(
                                name: "kestrel-loop-smoke",
                                partitions: 2,
                                replicationFactor: 1,
                                configs: ["retention.ms": "86400000"]
                            )
                            print("DUMP_CREATED kestrel-loop-smoke created=\(created) present=\(connection.topic(named: "kestrel-loop-smoke") != nil)")
                        } catch {
                            print("DUMP_CREATE_FAILED \(ClusterConnection.message(for: error))")
                        }
                    }
                case "delete-smoke-topic":
                    if let clusterID = store.selection?.clusterID,
                       let connection = store.connection(for: clusterID) {
                        do {
                            try await connection.deleteTopic(name: "kestrel-loop-smoke")
                            print("DUMP_DELETED kestrel-loop-smoke present=\(connection.topic(named: "kestrel-loop-smoke") != nil)")
                        } catch {
                            print("DUMP_DELETE_FAILED \(ClusterConnection.message(for: error))")
                        }
                    }
                case "open-find":
                    if let clusterID = store.selection?.clusterID {
                        var topic: String?
                        if case .topic(_, let name) = store.selection { topic = name }
                        store.findTarget = FindTarget(clusterID: clusterID, topic: topic)
                    }
                case "open-generate":
                    if let clusterID = store.selection?.clusterID {
                        var topic: String?
                        if case .topic(_, let name) = store.selection { topic = name }
                        store.generateTarget = GenerateTarget(clusterID: clusterID, topic: topic)
                    }
                case "open-export":
                    if let clusterID = store.selection?.clusterID {
                        var topic: String?
                        if case .topic(_, let name) = store.selection { topic = name }
                        store.exportTarget = ExportTarget(clusterID: clusterID, topic: topic)
                    }
                case "open-import":
                    if let clusterID = store.selection?.clusterID {
                        var topic: String?
                        if case .topic(_, let name) = store.selection { topic = name }
                        store.importTarget = ImportTarget(clusterID: clusterID, topic: topic)
                    }
                case "show-messages":
                    store.topicPane = .messages
                case "select-record":
                    if case .topic(let clusterID, let name) = store.selection,
                       let connection = store.connection(for: clusterID),
                       let topic = connection.topic(named: name),
                       let page = connection.page(
                           for: ClusterConnection.PartitionKey(
                               topic: name,
                               partition: topic.partitions.first?.id ?? 0
                           )
                       ) {
                        store.selectedRecord = page.records.last?.id
                        print("DUMP_SELECTED_RECORD \(store.selectedRecord ?? "none")")
                    }
                case "save-record":
                    // Exercises the same writer the Save menu uses, without a
                    // save panel. Destination comes from KESTREL_SNAPSHOT_SAVE.
                    if case .topic(let clusterID, let name) = store.selection,
                       let connection = store.connection(for: clusterID),
                       let topic = connection.topic(named: name),
                       let page = connection.page(
                           for: ClusterConnection.PartitionKey(
                               topic: name,
                               partition: topic.partitions.first?.id ?? 0
                           )
                       ),
                       let record = page.records.last,
                       let directory = ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_SAVE"] {
                        for format in RecordSaveFormat.allCases {
                            let url = URL(fileURLWithPath: directory)
                                .appendingPathComponent(
                                    RecordSaver.suggestedName(
                                        record: record,
                                        topic: name,
                                        format: format
                                    )
                                )
                            do {
                                let bytes = try RecordFile.write(
                                    record: record,
                                    topic: name,
                                    format: format,
                                    to: url
                                )
                                print("DUMP_SAVED format=\(format.rawValue) bytes=\(bytes) file=\(url.lastPathComponent)")
                            } catch {
                                print("DUMP_SAVE_FAILED \(format.rawValue) \(error.localizedDescription)")
                            }
                        }
                        print("DUMP_SAVE_SOURCE offset=\(record.offset) valueBytes=\(record.valueByteCount)")
                    }
                case "expand-connect":
                    if let id = store.selection?.clusterID {
                        store.setExpanded(.connectFolder(id), true)
                        store.selection = .connectFolder(id)
                        await store.connection(for: id)?.loadConnectors(force: true)
                    }
                case "select-connector":
                    // KESTREL_SNAPSHOT_CONNECTOR picks one by name; without it
                    // the first connector needing attention is chosen, since
                    // that is the interesting one to look at.
                    if let id = store.selection?.clusterID,
                       let connection = store.connection(for: id) {
                        let wanted = ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_CONNECTOR"]
                        let connectors = connection.connectors
                        let connector = wanted.flatMap { name in
                            connectors.first { $0.name == name }
                        } ?? connectors.first { $0.needsAttention } ?? connectors.first
                        if let connector { store.selection = .connector(id, connector.name) }
                    }
                case "connector-pause", "connector-resume", "connector-restart":
                    if case .connector(let id, let name) = store.selection,
                       let connection = store.connection(for: id) {
                        let requested: ClusterConnection.ConnectorAction = switch action {
                        case "connector-pause": .pause
                        case "connector-resume": .resume
                        default: .restart
                        }
                        let failure = await connection.run(requested, on: name)
                        print("DUMP_CONNECT_ACTION \(requested.rawValue) \(failure ?? "accepted")")
                    }
                case "dump-connect":
                    dumpConnect()
                case "dump-topics":
                    dumpTopics()
                case "dump-selection":
                    dumpSelection()
                case "dump-avro":
                    await dumpAvro()
                case "load-groups":
                    if let id = store.selection?.clusterID {
                        await store.connection(for: id)?.loadGroups()
                    }
                default:
                    print("SNAPSHOT_UNKNOWN_ACTION=\(action)")
                }
            }
            await Snapshot.runIfRequested()
        }
    }
}

private extension RootView {
    /// Prints the topics the sidebar shows, for comparing with the CLI.
    ///
    /// The acceptance line for the CLI slice is that `kestrel topics` lists
    /// what the GUI lists, which is only worth asserting if both sides can be
    /// diffed rather than eyeballed.
    func dumpTopics() {
        guard let clusterID = store.selection?.clusterID else { return }
        let topics = store.visibleTopics(for: clusterID)
        print("DUMP_TOPIC_COUNT \(topics.count)")
        for topic in topics {
            print("DUMP_TOPIC_ROW \(topic.name)\t\(topic.partitionCount)")
        }
    }

    /// Prints the Connect branch and, if one is selected, the connector.
    ///
    /// The list and the load state both matter here: the acceptance line is
    /// that an unreachable worker is explained, and only the load state can
    /// tell "no connectors" apart from "could not ask".
    func dumpConnect() {
        guard let clusterID = store.selection?.clusterID,
              let connection = store.connection(for: clusterID)
        else { return }

        let url = connection.profile.connect.url
        print("DUMP_CONNECT url=\(url.isEmpty ? "none" : url) load=\(connection.connectorsLoad) connectors=\(connection.connectors.count)")

        for connector in connection.connectors {
            let running = connector.tasks.filter { $0.state == .running }.count
            print("DUMP_CONNECTOR name=\(connector.name) kind=\(connector.kind.rawValue) state=\(connector.state.label) tasks=\(running)/\(connector.tasks.count) attention=\(connector.needsAttention) class=\(connector.connectorClass ?? "-")")
        }

        if case .connector(_, let name) = store.selection, let connector = connection.connector(named: name) {
            print("DUMP_CONNECTOR_DETAIL name=\(connector.name) worker=\(connector.workerID) configs=\(connector.config.count) failedTasks=\(connector.failedTasks.count)")
            for task in connector.tasks {
                let trace = task.trace?.split(separator: "\n").first.map(String.init) ?? "none"
                print("DUMP_CONNECT_TASK id=\(task.id) state=\(task.state.label) worker=\(task.workerID) trace=\(trace.prefix(90))")
            }
        }
    }

    /// Prints what the detail pane's Avro block is showing.
    ///
    /// Separate from `dump-selection` because decoding needs the registry, and
    /// so has to await. Reports the same three states the pane has: not Avro,
    /// decoded, or a reason it could not be.
    func dumpAvro() async {
        guard case .topic(let clusterID, let name) = store.selection,
              let connection = store.connection(for: clusterID)
        else { return }

        let partition = connection.topic(named: name)?.partitions.first?.id ?? 0
        let key = ClusterConnection.PartitionKey(topic: name, partition: partition)
        let records = connection.page(for: key)?.records ?? []
        guard let record = records.first(where: { $0.id == store.selectedRecord }) ?? records.first
        else {
            print("DUMP_AVRO none=no-record")
            return
        }

        let schemaID = AvroPayload.schemaID(of: record.value).map(String.init) ?? "-"
        let registry = connection.profile.schemaRegistry.url
        print("DUMP_AVRO_SOURCE topic=\(name) offset=\(record.offset) framed=\(AvroPayload.isFramed(record.value)) schemaID=\(schemaID) registry=\(registry.isEmpty ? "none" : registry)")

        do {
            if let decoded = try await connection.decodeAvro(record.value) {
                let lines = decoded.json.split(separator: "\n")
                print("DUMP_AVRO decoded schemaID=\(decoded.schemaID) lines=\(lines.count) schemaBytes=\(decoded.schemaText.count)")
                for line in lines.prefix(12) {
                    print("DUMP_AVRO_JSON \(line.trimmingCharacters(in: .whitespaces))")
                }
            } else {
                print("DUMP_AVRO not-avro")
            }
        } catch {
            let message = (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
            print("DUMP_AVRO failed \(message)")
        }
    }

    /// Prints what the inspector is showing for the current selection.
    ///
    /// Screenshots cannot be trusted on this machine — without the Screen
    /// Recording permission the window server hands back an empty buffer, and
    /// the permission-free fallback loses text — so UI state is also asserted
    /// in text. Driven by the `dump-selection` snapshot action.
    func dumpSelection() {
        switch store.selection {
        case .group(let clusterID, let groupID):
            guard let connection = store.connection(for: clusterID),
                  let group = connection.group(id: groupID) else { return }
            let rows = connection.offsets(forGroup: groupID)
            print("""
                DUMP_GROUP id=\(group.id) state=\(group.state) members=\(group.members.count) \
                topics=\(group.subscribedTopics.count) partitions=\(rows.count) \
                load=\(connection.offsetLoad(forGroup: groupID)) \
                totalLag=\(rows.compactMap(\.lag).reduce(0, +))
                """)
            for row in rows.prefix(40) {
                let committed = row.committed.map(String.init) ?? "-"
                let lag = row.lag.map(String.init) ?? "-"
                print("DUMP_OFFSET \(row.topic)|\(row.partition)|\(committed)|\(row.logEnd)|\(lag)")
            }
            if let member = group.members.first(where: { !$0.assignments.isEmpty }) {
                print("""
                    DUMP_MEMBER client=\(member.clientId) host=\(member.clientHost) \
                    partitions=\(member.partitionCount) topics=\(member.topics.joined(separator: ","))
                    """)
            }

        case .topic(let clusterID, let name):
            guard let connection = store.connection(for: clusterID),
                  let topic = connection.topic(named: name) else { return }
            print("DUMP_TOPIC name=\(topic.name) partitions=\(topic.partitionCount)")
            for partition in topic.partitions {
                print("""
                    DUMP_PARTITION id=\(partition.id) leader=\(partition.leader) \
                    replicas=\(partition.replicas) isr=\(partition.inSyncReplicas)
                    """)
            }
            let configs = connection.configs(for: .topic(name))
            print("DUMP_TOPIC_CONFIGS count=\(configs.count) overrides=\(configs.filter { !$0.isDefault }.count)")
            for entry in configs.filter({ !$0.isDefault }).prefix(5) {
                print("DUMP_CONFIG \(entry.name)=\(entry.displayValue)")
            }

            let partition = topic.partitions.first?.id ?? 0
            let key = ClusterConnection.PartitionKey(topic: name, partition: partition)
            print("DUMP_RECORDS_LOAD \(connection.recordLoad(for: key))")
            print("DUMP_TOAST \(store.toast?.replacingOccurrences(of: "\n", with: " ") ?? "none")")
            if let page = connection.page(for: key) {
                print("""
                    DUMP_PAGE partition=\(partition) records=\(page.records.count) \
                    start=\(page.startOffset) low=\(page.lowWatermark) high=\(page.highWatermark) \
                    reachedEnd=\(page.reachedEnd) empty=\(page.isPartitionEmpty)
                    """)
            if let record = connection.page(for: key)?.records.first {
                let value = PayloadFormatter.format(record.value)
                let keyPayload = PayloadFormatter.format(record.key)
                print("""
                    DUMP_PAYLOAD valueKind=\(value.kind.rawValue) \
                    prettyLines=\(value.pretty?.split(separator: "\n").count ?? 0) \
                    rawLines=\(value.raw.split(separator: "\n").count) \
                    malformed=\(value.isMalformed) problem=\(value.problem ?? "none") \
                    keyKind=\(keyPayload.kind.rawValue)
                    """)
                for line in (value.pretty ?? value.raw).split(separator: "\n").prefix(4) {
                    print("DUMP_PRETTY \(line)")
                }
            }

                for record in page.records.prefix(3) {
                    print("""
                        DUMP_RECORD offset=\(record.offset) \
                        timestamp=\(record.timestamp?.description ?? "none") \
                        headers=\(record.headers.count) bytes=\(record.valueByteCount) \
                        key=\(record.keyPreview.prefix(40)) value=\(record.valuePreview.prefix(60))
                        """)
                }
            }

        case .broker(let clusterID, let brokerID):
            guard let connection = store.connection(for: clusterID),
                  let broker = connection.broker(id: brokerID) else { return }
            let configs = connection.configs(for: .broker(brokerID))
            print("""
                DUMP_BROKER id=\(broker.id) host=\(broker.host) port=\(broker.port) \
                rack=\(connection.rack(forBroker: brokerID) ?? "not set") \
                configs=\(configs.count) sensitive=\(configs.filter(\.isSensitive).count)
                """)

        default:
            print("DUMP_SELECTION \(String(describing: store.selection))")
        }
    }
}

/// Routes the sidebar selection to the right inspector.
struct DetailView: View {
    @Environment(ClusterStore.self) private var store

    var body: some View {
        switch store.selection {
        case .cluster(let id):
            if let cluster = store.cluster(id: id) {
                ClusterInspector(cluster: cluster)
            } else {
                emptyState
            }

        case .brokersFolder(let id):
            listSummary(
                title: "Brokers",
                systemImage: "rectangle.stack",
                rows: (store.connection(for: id)?.brokers ?? []).map {
                    ("Broker \($0.id)", $0.endpoint)
                }
            )

        case .topicsFolder(let id):
            listSummary(
                title: "Topics",
                systemImage: "tray.2",
                rows: store.visibleTopics(for: id).map {
                    ($0.name, "\($0.partitionCount) partitions")
                }
            )

        case .groupsFolder(let id):
            listSummary(
                title: "Consumer Groups",
                systemImage: "person.2.badge.gearshape",
                rows: (store.connection(for: id)?.groups ?? []).map {
                    ($0.id, $0.state)
                }
            )

        case .connectFolder(let id):
            ConnectFolderInspector(clusterID: id)

        case .connector(let clusterID, let name):
            if let connector = store.connection(for: clusterID)?.connector(named: name) {
                ConnectorInspector(clusterID: clusterID, connector: connector)
            } else {
                emptyState
            }

        case .broker(let clusterID, let brokerID):
            if let broker = store.connection(for: clusterID)?.broker(id: brokerID) {
                BrokerInspector(clusterID: clusterID, broker: broker)
            } else {
                emptyState
            }

        case .topic(let clusterID, let name):
            if let topic = store.connection(for: clusterID)?.topic(named: name) {
                TopicInspector(clusterID: clusterID, topic: topic)
            } else {
                emptyState
            }

        case .group(let clusterID, let groupID):
            if let group = store.connection(for: clusterID)?.group(id: groupID) {
                GroupInspector(clusterID: clusterID, group: group)
                .navigationTitle(group.id)
            } else {
                emptyState
            }

        case nil:
            emptyState
        }
    }

    private var emptyState: some View {
        ContentUnavailableView {
            Label("Nothing Selected", systemImage: "point.3.connected.trianglepath.dotted")
        } description: {
            Text("Pick a cluster in the sidebar, or add one.")
        } actions: {
            Button("Add Cluster") { store.beginAdd() }
                .buttonStyle(.borderedProminent)
        }
    }

    private func listSummary(
        title: String,
        systemImage: String,
        rows: [(String, String)]
    ) -> some View {
        Form {
            Section(title) {
                if rows.isEmpty {
                    Label("Nothing loaded yet", systemImage: systemImage)
                        .foregroundStyle(.secondary)
                } else {
                    ForEach(rows, id: \.0) { row in
                        LabeledContent(row.0, value: row.1)
                    }
                }
            }
        }
        .formStyle(.grouped)
        .navigationTitle(title)
    }
}

/// Connection settings and state for one cluster.
struct ClusterInspector: View {
    let cluster: ClusterProfile

    @Environment(ClusterStore.self) private var store

    private var connection: ClusterConnection? { store.connection(for: cluster.id) }

    var body: some View {
        Form {
            Section("Connection") {
                LabeledContent("Name", value: cluster.name)
                LabeledContent("Bootstrap Servers", value: cluster.bootstrapServers)
                LabeledContent("Security Protocol", value: cluster.securityProtocol.rawValue)
                if cluster.securityProtocol.usesSASL {
                    LabeledContent("SASL Mechanism", value: cluster.saslMechanism?.rawValue ?? "Not set")
                    LabeledContent("Username", value: cluster.saslUsername.isEmpty ? "—" : cluster.saslUsername)
                }
                if cluster.securityProtocol.usesTLS {
                    LabeledContent("Verify Hostname", value: cluster.tls.verifyHostname ? "Yes" : "No")
                }
            }

            Section("Status") {
                ConnectionPhaseRow(connection: connection)
                HStack {
                    if connection?.isConnected == true {
                        Button("Refresh") { Task { await store.refresh(id: cluster.id) } }
                        Button("Disconnect") { store.disconnect(id: cluster.id) }
                    } else {
                        Button("Connect") { Task { await store.connect(id: cluster.id) } }
                            .disabled(connection?.phase == .connecting)
                    }
                    Button("Edit Cluster…") { store.beginEdit(id: cluster.id) }
                }
            }

            if let connection, connection.isConnected {
                Section("Cluster") {
                    LabeledContent("Brokers", value: "\(connection.brokers.count)")
                    LabeledContent("Topics", value: "\(connection.topics.count)")
                    if let refreshed = connection.lastRefreshed {
                        LabeledContent(
                            "Last Refreshed",
                            value: refreshed.formatted(date: .omitted, time: .standard)
                        )
                    }
                }
            }
        }
        .formStyle(.grouped)
        .navigationTitle(cluster.name)
    }
}

/// Shows whether a cluster is connected, and why it is not.
struct ConnectionPhaseRow: View {
    let connection: ClusterConnection?

    var body: some View {
        switch connection?.phase {
        case .connected:
            let brokers = connection?.brokers ?? []
            Label {
                Text("Connected to \(brokers.count) broker\(brokers.count == 1 ? "" : "s") (\(brokers.map(\.endpoint).joined(separator: ", ")))")
            } icon: {
                Image(systemName: "checkmark.circle.fill").foregroundStyle(.green)
            }
        case .connecting:
            HStack(spacing: 8) {
                ProgressView().controlSize(.small)
                Text("Connecting…")
            }
        case .failed(let message):
            Label {
                Text(message).textSelection(.enabled)
            } icon: {
                Image(systemName: "exclamationmark.triangle.fill").foregroundStyle(.orange)
            }
        case .idle, nil:
            Label("Not connected", systemImage: "circle.dashed")
                .foregroundStyle(.secondary)
        }
    }
}


/// Cluster, produce and topic editor sheets.
private struct EditorSheets: ViewModifier {
    @Environment(ClusterStore.self) private var store

    func body(content: Content) -> some View {
        content
            .sheet(item: Binding(get: { store.draft }, set: { store.draft = $0 })) { draft in
                ClusterEditorSheet(
                    draft: draft,
                    onSave: { store.commit($0) },
                    onCancel: { store.draft = nil }
                )
            }
            .sheet(
                item: Binding(get: { store.produceTarget }, set: { store.produceTarget = $0 })
            ) { target in
                ProduceSheet(target: target)
            }
    }
}

/// Topic creation, widening and record deletion, plus the delete confirmation.
private struct TopicManagementSheets: ViewModifier {
    @Environment(ClusterStore.self) private var store

    func body(content: Content) -> some View {
        content
            .sheet(
                item: Binding(get: { store.topicSheet }, set: { store.topicSheet = $0 })
            ) { sheet in
                switch sheet {
                case .create(let clusterID):
                    CreateTopicSheet(clusterID: clusterID)
                case .addPartitions(let clusterID, let topic):
                    AddPartitionsSheet(clusterID: clusterID, topic: topic)
                case .deleteRecords(let clusterID, let topic):
                    DeleteRecordsSheet(clusterID: clusterID, topic: topic)
                }
            }
            .sheet(
                item: Binding(get: { store.importTarget }, set: { store.importTarget = $0 })
            ) { target in
                ImportSheet(target: target)
            }
            .sheet(
                item: Binding(get: { store.exportTarget }, set: { store.exportTarget = $0 })
            ) { target in
                ExportSheet(target: target)
            }
            .sheet(
                item: Binding(get: { store.generateTarget }, set: { store.generateTarget = $0 })
            ) { target in
                GenerateSheet(target: target)
            }
            .sheet(
                item: Binding(get: { store.findTarget }, set: { store.findTarget = $0 })
            ) { target in
                FindSheet(target: target)
            }
            .confirmationDialog(
                "Delete topic \(store.topicDeletion?.topic ?? "")?",
                isPresented: Binding(
                    get: { store.topicDeletion != nil },
                    set: { if !$0 { store.topicDeletion = nil } }
                ),
                titleVisibility: .visible,
                presenting: store.topicDeletion
            ) { deletion in
                Button("Delete Topic", role: .destructive) { delete(deletion) }
                Button("Cancel", role: .cancel) { store.topicDeletion = nil }
            } message: { deletion in
                Text("Every record in \(deletion.topic) is discarded. This cannot be undone.")
            }
    }

    private func delete(_ deletion: TopicDeletion) {
        Task {
            guard let connection = store.connection(for: deletion.clusterID) else { return }
            do {
                try await connection.deleteTopic(name: deletion.topic)
                // The selection pointed at a topic that no longer exists.
                if case .topic(_, let name) = store.selection, name == deletion.topic {
                    store.selection = .topicsFolder(deletion.clusterID)
                }
                store.toast = "Deleted topic \(deletion.topic)"
            } catch {
                store.errorMessage = ClusterConnection.message(for: error)
            }
            store.topicDeletion = nil
        }
    }
}

/// The transient toast and the error alert.
private struct Notices: ViewModifier {
    @Environment(ClusterStore.self) private var store

    func body(content: Content) -> some View {
        content
            .overlay(alignment: .bottom) {
                if let toast = store.toast {
                    ToastView(text: toast)
                }
            }
            .animation(.default, value: store.toast)
            .task(id: store.toast) {
                guard store.toast != nil else { return }
                try? await Task.sleep(for: .seconds(4))
                store.toast = nil
            }
            .alert(
                "Kestrel",
                isPresented: Binding(
                    get: { store.errorMessage != nil },
                    set: { if !$0 { store.errorMessage = nil } }
                )
            ) {
                Button("OK") { store.errorMessage = nil }
            } message: {
                Text(store.errorMessage ?? "")
            }
    }
}

/// Shows the outcome of a connection test started from the editor sheet.
struct ConnectionStatusRow: View {
    let state: ClusterStore.ConnectionState

    var body: some View {
        switch state {
        case .idle:
            Label("Not tested", systemImage: "circle.dashed")
                .foregroundStyle(.secondary)
        case .testing:
            HStack(spacing: 8) {
                ProgressView().controlSize(.small)
                Text("Connecting…")
            }
        case .tested(let result):
            Label {
                Text(result.summary).textSelection(.enabled)
            } icon: {
                Image(systemName: result.isSuccess ? "checkmark.circle.fill" : "exclamationmark.triangle.fill")
                    .foregroundStyle(result.isSuccess ? Color.green : Color.orange)
            }
        }
    }
}
