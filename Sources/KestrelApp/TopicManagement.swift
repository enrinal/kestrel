import KestrelKit
import SwiftUI

/// Which topic-management sheet is open.
enum TopicSheet: Identifiable {
    case create(UUID)
    case addPartitions(UUID, TopicInfo)
    case deleteRecords(UUID, TopicInfo)

    var id: String {
        switch self {
        case .create(let cluster): "create:\(cluster)"
        case .addPartitions(_, let topic): "partitions:\(topic.name)"
        case .deleteRecords(_, let topic): "records:\(topic.name)"
        }
    }
}

/// A topic awaiting delete confirmation.
struct TopicDeletion: Identifiable, Equatable {
    let clusterID: UUID
    let topic: String

    var id: String { "\(clusterID):\(topic)" }
}

/// One editable topic config override.
private struct ConfigDraft: Identifiable, Equatable {
    let id = UUID()
    var name = ""
    var value = ""
}

/// Creates a topic with a partition count, replication factor and overrides.
struct CreateTopicSheet: View {
    let clusterID: UUID

    @Environment(ClusterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    @State private var name = ""
    @State private var partitions = 1
    @State private var replicationFactor = 1
    @State private var configs: [ConfigDraft] = []
    @State private var isCreating = false
    @State private var failure: String?

    private var connection: ClusterConnection? { store.connection(for: clusterID) }
    private var brokerCount: Int { connection?.brokers.count ?? 1 }

    /// Kafka rejects a topic whose replication factor exceeds the broker count.
    private var replicationExceedsBrokers: Bool { replicationFactor > brokerCount }

    private var isValid: Bool {
        !name.isEmpty
            && partitions >= 1
            && replicationFactor >= 1
            && !replicationExceedsBrokers
            && configs.allSatisfy { !$0.name.isEmpty }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Create Topic")
                .font(.headline)

            TextField("Name", text: $name)
                .textFieldStyle(.roundedBorder)

            HStack(spacing: 20) {
                Stepper("Partitions: \(partitions)", value: $partitions, in: 1...1000)
                    .frame(width: 180)
                Stepper(
                    "Replication: \(replicationFactor)",
                    value: $replicationFactor,
                    in: 1...100
                )
                .frame(width: 190)
                Spacer()
            }

            if replicationExceedsBrokers {
                Label(
                    "This cluster has \(brokerCount) broker\(brokerCount == 1 ? "" : "s"), so the replication factor cannot exceed \(brokerCount).",
                    systemImage: "exclamationmark.triangle"
                )
                .font(.callout)
                .foregroundStyle(.orange)
            }

            VStack(alignment: .leading, spacing: 6) {
                HStack {
                    Text("Configuration overrides")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Spacer()
                    Button {
                        configs.append(ConfigDraft())
                    } label: {
                        Label("Add", systemImage: "plus")
                    }
                    .buttonStyle(.borderless)
                }

                if configs.isEmpty {
                    Text("Broker defaults will be used.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                } else {
                    ForEach($configs) { $config in
                        HStack {
                            TextField("cleanup.policy", text: $config.name)
                            TextField("compact", text: $config.value)
                            Button {
                                configs.removeAll { $0.id == config.id }
                            } label: {
                                Image(systemName: "minus.circle")
                            }
                            .buttonStyle(.borderless)
                        }
                    }
                }
            }

            if let failure {
                Text(failure)
                    .font(.callout)
                    .foregroundStyle(.red)
                    .textSelection(.enabled)
            }

            Spacer()

            HStack {
                if isCreating { ProgressView().controlSize(.small) }
                Spacer()
                Button("Cancel") { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button("Create") { create() }
                    .keyboardShortcut(.defaultAction)
                    .buttonStyle(.borderedProminent)
                    .disabled(isCreating || !isValid)
            }
        }
        .padding(16)
        .frame(width: 520, height: 420)
    }

    private func create() {
        guard let connection else { return }
        isCreating = true
        failure = nil

        Task {
            do {
                let overrides = Dictionary(
                    configs.map { ($0.name, $0.value) },
                    uniquingKeysWith: { _, last in last }
                )
                let created = try await connection.createTopic(
                    name: name,
                    partitions: Int32(partitions),
                    replicationFactor: Int32(replicationFactor),
                    configs: overrides
                )
                store.toast = created
                    ? "Created topic \(name)"
                    : "Topic \(name) already existed"
                dismiss()
            } catch {
                failure = ClusterConnection.message(for: error)
            }
            isCreating = false
        }
    }
}

/// Raises a topic's partition count.
struct AddPartitionsSheet: View {
    let clusterID: UUID
    let topic: TopicInfo

    @Environment(ClusterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    @State private var total: Int
    @State private var isApplying = false
    @State private var failure: String?

    init(clusterID: UUID, topic: TopicInfo) {
        self.clusterID = clusterID
        self.topic = topic
        // Starts one above the current count, the smallest legal change.
        self._total = State(initialValue: topic.partitionCount + 1)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Add Partitions")
                .font(.headline)
            Text(topic.name)
                .font(.caption)
                .foregroundStyle(.secondary)

            Stepper(
                "New total: \(total)",
                value: $total,
                in: (topic.partitionCount + 1)...1000
            )
            .frame(width: 220)

            Text("Currently \(topic.partitionCount); adding \(total - topic.partitionCount).")
                .font(.callout)
                .foregroundStyle(.secondary)

            Label(
                """
                Kafka cannot remove partitions later, and adding them changes which \
                partition a key hashes to. Existing records stay where they are.
                """,
                systemImage: "exclamationmark.triangle"
            )
            .font(.callout)
            .foregroundStyle(.orange)

            if let failure {
                Text(failure)
                    .font(.callout)
                    .foregroundStyle(.red)
                    .textSelection(.enabled)
            }

            Spacer()

            HStack {
                if isApplying { ProgressView().controlSize(.small) }
                Spacer()
                Button("Cancel") { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button("Add Partitions") { apply() }
                    .buttonStyle(.borderedProminent)
                    .disabled(isApplying)
            }
        }
        .padding(16)
        .frame(width: 480, height: 320)
    }

    private func apply() {
        guard let connection = store.connection(for: clusterID) else { return }
        isApplying = true
        failure = nil

        Task {
            do {
                try await connection.createPartitions(topic: topic.name, totalCount: total)
                store.toast = "\(topic.name) now has \(total) partitions"
                dismiss()
            } catch {
                failure = ClusterConnection.message(for: error)
            }
            isApplying = false
        }
    }
}

/// Trims the start of a partition by deleting records below an offset.
struct DeleteRecordsSheet: View {
    let clusterID: UUID
    let topic: TopicInfo

    @Environment(ClusterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    @State private var partition: Int32 = 0
    @State private var scope = Scope.all
    @State private var offsetText = "0"
    @State private var isConfirming = false
    @State private var isApplying = false
    @State private var failure: String?

    private enum Scope: String, CaseIterable, Identifiable {
        case all = "Every record"
        case beforeOffset = "Before offset"

        var id: String { rawValue }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Delete Records")
                .font(.headline)
            Text(topic.name)
                .font(.caption)
                .foregroundStyle(.secondary)

            HStack {
                Picker("Partition", selection: $partition) {
                    ForEach(topic.partitions) { Text("\($0.id)").tag($0.id) }
                }
                .frame(width: 160)
                Spacer()
            }

            Picker("Delete", selection: $scope) {
                ForEach(Scope.allCases) { Text($0.rawValue).tag($0) }
            }
            .pickerStyle(.radioGroup)

            if scope == .beforeOffset {
                TextField("Offset", text: $offsetText)
                    .frame(width: 140)
                    .monospacedDigit()
            }

            Text(
                """
                Only whole log segments are reclaimed, so the broker may keep more \
                than you ask it to. This raises the partition's low watermark; it \
                does not rewrite the log.
                """
            )
            .font(.callout)
            .foregroundStyle(.secondary)

            if let failure {
                Text(failure)
                    .font(.callout)
                    .foregroundStyle(.red)
                    .textSelection(.enabled)
            }

            Spacer()

            HStack {
                if isApplying { ProgressView().controlSize(.small) }
                Spacer()
                Button("Cancel") { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button("Delete Records…") { isConfirming = true }
                    .buttonStyle(.borderedProminent)
                    .disabled(isApplying)
            }
        }
        .padding(16)
        .frame(width: 500, height: 380)
        .confirmationDialog(
            scope == .all
                ? "Delete every record in \(topic.name) partition \(partition)?"
                : "Delete records before offset \(offsetText) in \(topic.name) partition \(partition)?",
            isPresented: $isConfirming,
            titleVisibility: .visible
        ) {
            Button("Delete Records", role: .destructive) { apply() }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("This cannot be undone.")
        }
    }

    private func apply() {
        guard let connection = store.connection(for: clusterID) else { return }
        isApplying = true
        failure = nil

        Task {
            do {
                let low = try await connection.deleteRecords(
                    topic: topic.name,
                    partition: partition,
                    beforeOffset: scope == .all ? nil : Int64(offsetText)
                )
                store.toast = "\(topic.name) partition \(partition) now starts at offset \(low)"
                dismiss()
            } catch {
                failure = ClusterConnection.message(for: error)
            }
            isApplying = false
        }
    }
}
