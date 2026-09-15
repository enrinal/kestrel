import KestrelKit
import SwiftUI

/// Broker identity plus its readable configuration.
struct BrokerInspector: View {
    let clusterID: UUID
    let broker: BrokerInfo

    @Environment(ClusterStore.self) private var store

    private var connection: ClusterConnection? { store.connection(for: clusterID) }
    private var resource: ConfigResource { .broker(broker.id) }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            InspectorHeader(title: "Broker \(broker.id)", systemImage: "cpu") {
                InspectorField("Host", broker.host)
                InspectorField("Port", "\(broker.port)")
                InspectorField("Rack", connection?.rack(forBroker: broker.id) ?? "Not set")
            }

            Divider()

            ConfigTable(
                entries: connection?.configs(for: resource) ?? [],
                load: connection?.configLoad(for: resource) ?? .notLoaded,
                onReload: { Task { await connection?.loadConfigs(for: resource, force: true) } }
            )
        }
        .navigationTitle("Broker \(broker.id)")
        .task(id: broker.id) {
            await connection?.loadConfigs(for: resource)
        }
    }
}

/// Topic partitions, replication, ISR, and configuration.
struct TopicInspector: View {
    let clusterID: UUID
    let topic: TopicInfo

    @Environment(ClusterStore.self) private var store

    private var connection: ClusterConnection? { store.connection(for: clusterID) }
    private var resource: ConfigResource { .topic(topic.name) }

    /// Replication factor, taken from the first partition's replica list.
    ///
    /// Kafka allows partitions to differ after a reassignment, so this is the
    /// nominal factor rather than a guarantee.
    private var replicationFactor: Int {
        topic.partitions.first?.replicas.count ?? 0
    }

    /// The topic's cleanup policy, once its configs have been read.
    private var cleanupPolicy: String? {
        connection?.configs(for: resource).first { $0.name == "cleanup.policy" }?.value
    }

    /// Whether DeleteRecords is allowed here.
    ///
    /// Kafka answers a compact-only topic with a policy violation, so the
    /// control is disabled rather than offered and then failing. The policy is
    /// only known once configs have loaded; until then the control stays live
    /// rather than being wrongly disabled.
    private var allowsRecordDeletion: Bool {
        guard let cleanupPolicy else { return true }
        return cleanupPolicy.contains("delete")
    }

    private var recordDeletionExplanation: String {
        if allowsRecordDeletion {
            return "Raise the partition's low watermark by deleting older records."
        }
        return """
            Unavailable: cleanup.policy is "\(cleanupPolicy ?? "compact")". Kafka refuses \
            DeleteRecords on a compacted topic with a policy violation.
            """
    }

    private var underReplicated: [PartitionInfo] {
        topic.partitions.filter { $0.inSyncReplicas.count < $0.replicas.count }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            InspectorHeader(title: topic.name, systemImage: "tray.full") {
                InspectorField("Partitions", "\(topic.partitionCount)")
                InspectorField("Replication Factor", "\(replicationFactor)")
                InspectorField("Internal", topic.isInternal ? "Yes" : "No")
                if !underReplicated.isEmpty {
                    InspectorField("Under-replicated", "\(underReplicated.count)", isWarning: true)
                }
                if let error = topic.error {
                    InspectorField("Error", error, isWarning: true)
                }

                Spacer()

                VStack(alignment: .trailing, spacing: 6) {
                    Button("Add Partitions…") {
                        store.topicSheet = .addPartitions(clusterID, topic)
                    }

                    Button("Delete Records…") {
                        store.topicSheet = .deleteRecords(clusterID, topic)
                    }
                    .disabled(!allowsRecordDeletion)
                    .help(recordDeletionExplanation)

                    Button("Delete Topic…", role: .destructive) {
                        store.topicDeletion = TopicDeletion(clusterID: clusterID, topic: topic.name)
                    }
                }
            }

            Picker(
                "View",
                selection: Binding(get: { store.topicPane }, set: { store.topicPane = $0 })
            ) {
                ForEach(TopicPane.allCases) { Text($0.rawValue).tag($0) }
            }
            .pickerStyle(.segmented)
            .labelsHidden()
            .padding(.horizontal, 16)
            .padding(.bottom, 8)

            Divider()

            switch store.topicPane {
            case .partitions:
                PartitionTable(partitions: topic.partitions)
            case .messages:
                MessageBrowser(clusterID: clusterID, topic: topic)
            case .configuration:
                ConfigTable(
                    entries: connection?.configs(for: resource) ?? [],
                    load: connection?.configLoad(for: resource) ?? .notLoaded,
                    onReload: { Task { await connection?.loadConfigs(for: resource, force: true) } }
                )
            }
        }
        .navigationTitle(topic.name)
        .task(id: topic.name) {
            await connection?.loadConfigs(for: resource)
        }
    }
}

/// Offset, leader, replica and ISR view of a topic's partitions.
struct PartitionTable: View {
    let partitions: [PartitionInfo]

    var body: some View {
        Table(partitions) {
            TableColumn("Partition") { Text("\($0.id)").monospacedDigit() }
                .width(min: 70, ideal: 80)
            TableColumn("Leader") { partition in
                Text(partition.leader < 0 ? "none" : "\(partition.leader)")
                    .foregroundStyle(partition.leader < 0 ? .orange : .primary)
            }
            .width(min: 60, ideal: 70)
            TableColumn("Replicas") { Text($0.replicas.map(String.init).joined(separator: ", ")) }
            TableColumn("In-Sync Replicas") { partition in
                let isr = partition.inSyncReplicas
                let text = isr.map(String.init).joined(separator: ", ")
                if isr.count < partition.replicas.count {
                    Label(text, systemImage: "exclamationmark.triangle.fill")
                        .foregroundStyle(.orange)
                } else {
                    Text(text)
                }
            }
        }
        .overlay {
            if partitions.isEmpty {
                ContentUnavailableView(
                    "No Partitions",
                    systemImage: "square.split.2x2",
                    description: Text("The cluster reported no partitions for this topic.")
                )
            }
        }
    }
}

/// Searchable configuration table shared by the broker and topic inspectors.
struct ConfigTable: View {
    let entries: [ConfigEntry]
    let load: ClusterConnection.Load
    let onReload: () -> Void

    @State private var query = ""
    @State private var overridesOnly = false

    private var filtered: [ConfigEntry] {
        entries.filter { entry in
            if overridesOnly, entry.isDefault { return false }
            guard !query.isEmpty else { return true }
            return entry.name.localizedCaseInsensitiveContains(query)
                || (entry.value ?? "").localizedCaseInsensitiveContains(query)
        }
    }

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                TextField("Filter configuration", text: $query)
                    .textFieldStyle(.roundedBorder)
                    .frame(maxWidth: 260)
                Toggle("Overrides only", isOn: $overridesOnly)
                Spacer()
                Text("\(filtered.count) of \(entries.count)")
                    .foregroundStyle(.secondary)
                    .monospacedDigit()
                Button("Reload", systemImage: "arrow.clockwise", action: onReload)
                    .labelStyle(.iconOnly)
                    .disabled(load == .loading)
            }
            .padding(12)

            Divider()

            Table(filtered) {
                TableColumn("Name") { Text($0.name).monospaced() }
                    .width(min: 180, ideal: 260)
                TableColumn("Value") { Text($0.displayValue).monospaced() }
                    .width(min: 120, ideal: 240)
                TableColumn("Source") { entry in
                    Text(entry.isDefault ? "Default" : "Overridden")
                        .foregroundStyle(entry.isDefault ? .secondary : .primary)
                }
                .width(min: 80, ideal: 90)
                TableColumn("Read-only") { Text($0.isReadOnly ? "Yes" : "No") }
                    .width(min: 70, ideal: 80)
            }
            .overlay {
                switch load {
                case .loading:
                    ProgressView("Reading configuration…")
                case .failed(let message):
                    ContentUnavailableView {
                        Label("Configuration Unavailable", systemImage: "exclamationmark.triangle")
                    } description: {
                        Text(message)
                    } actions: {
                        Button("Try Again", action: onReload)
                    }
                case .notLoaded, .loaded:
                    if filtered.isEmpty {
                        ContentUnavailableView(
                            entries.isEmpty ? "No Configuration" : "No Matches",
                            systemImage: "slider.horizontal.3"
                        )
                    }
                }
            }
        }
    }
}

// MARK: Shared pieces

/// Title and key facts shown above an inspector's tables.
struct InspectorHeader<Content: View>: View {
    let title: String
    let systemImage: String
    @ViewBuilder let content: Content

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Label(title, systemImage: systemImage)
                .font(.title2.weight(.semibold))
                .textSelection(.enabled)
            HStack(alignment: .top, spacing: 24) {
                content
            }
        }
        .padding(16)
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

/// One labelled fact in an ``InspectorHeader``.
struct InspectorField: View {
    let label: String
    let value: String
    var isWarning = false

    init(_ label: String, _ value: String, isWarning: Bool = false) {
        self.label = label
        self.value = value
        self.isWarning = isWarning
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(label)
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(value)
                .foregroundStyle(isWarning ? .orange : .primary)
                .textSelection(.enabled)
        }
    }
}

/// The panes of the topic inspector.
enum TopicPane: String, CaseIterable, Identifiable {
    case partitions = "Partitions"
    case messages = "Messages"
    case configuration = "Configuration"

    var id: String { rawValue }
}
