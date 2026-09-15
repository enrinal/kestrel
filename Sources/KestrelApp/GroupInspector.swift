import KestrelKit
import SwiftUI

/// Shows a consumer group: its members, and where each partition is committed.
struct GroupInspector: View {
    let clusterID: UUID
    let group: ConsumerGroupInfo

    @Environment(ClusterStore.self) private var store

    @State private var pane = Pane.offsets
    @State private var isResetting = false

    private enum Pane: String, CaseIterable, Identifiable {
        case offsets = "Offsets"
        case members = "Members"

        var id: String { rawValue }
    }

    private var connection: ClusterConnection? { store.connection(for: clusterID) }
    private var offsets: [GroupPartitionOffset] { connection?.offsets(forGroup: group.id) ?? [] }
    private var load: ClusterConnection.Load {
        connection?.offsetLoad(forGroup: group.id) ?? .notLoaded
    }

    /// Total records behind, across every partition with a committed offset.
    private var totalLag: Int64 {
        offsets.compactMap(\.lag).reduce(0, +)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            InspectorHeader(title: group.id, systemImage: "person.2.badge.gearshape") {
                InspectorField("State", group.state)
                InspectorField("Protocol", group.protocolType.isEmpty ? "—" : group.protocolType)
                InspectorField("Members", "\(group.members.count)")
                InspectorField("Topics", "\(group.subscribedTopics.count)")
                InspectorField("Partitions", "\(offsets.count)")
                InspectorField(
                    "Total Lag",
                    load == .loaded ? "\(totalLag)" : "—",
                    isWarning: totalLag > 0
                )

                Spacer()

                VStack(alignment: .trailing, spacing: 6) {
                    Button {
                        Task { await connection?.loadOffsets(forGroup: group.id) }
                    } label: {
                        Label("Refresh", systemImage: "arrow.clockwise")
                    }
                    .disabled(load == .loading)

                    Button("Reset Offsets…") { isResetting = true }
                        .disabled(offsets.isEmpty)
                }
            }

            Divider()

            Picker("", selection: $pane) {
                ForEach(Pane.allCases) { Text($0.rawValue).tag($0) }
            }
            .pickerStyle(.segmented)
            .labelsHidden()
            .padding(12)

            switch pane {
            case .offsets:
                offsetTable
            case .members:
                memberTable
            }
        }
        .task(id: group.id) {
            // Offsets are a per-group request, so they load on selection
            // rather than with the rest of the cluster.
            if load == .notLoaded {
                await connection?.loadOffsets(forGroup: group.id)
            }
        }
        .sheet(isPresented: $isResetting) {
            ResetOffsetsSheet(clusterID: clusterID, group: group, offsets: offsets)
        }
    }

    private var offsetTable: some View {
        Table(offsets) {
            TableColumn("Topic") { Text($0.topic).lineLimit(1) }
                .width(min: 180, ideal: 300)
            TableColumn("Partition") { Text("\($0.partition)").monospacedDigit() }
                .width(min: 70, ideal: 80)
            TableColumn("Committed") { row in
                Text(row.committed.map(String.init) ?? "none").monospacedDigit()
            }
            .width(min: 90, ideal: 100)
            TableColumn("Log End") { Text("\($0.logEnd)").monospacedDigit() }
                .width(min: 80, ideal: 90)
            TableColumn("Lag") { row in
                Text(row.lag.map(String.init) ?? "—")
                    .monospacedDigit()
                    // Any lag at all is the number people are looking for.
                    .foregroundStyle((row.lag ?? 0) > 0 ? .orange : .primary)
            }
            .width(min: 60, ideal: 80)
        }
        .overlay { overlay }
    }

    @ViewBuilder
    private var overlay: some View {
        switch load {
        case .loading:
            ProgressView("Reading committed offsets…")
        case .failed(let message):
            ContentUnavailableView {
                Label("Could Not Read Offsets", systemImage: "exclamationmark.triangle")
            } description: {
                Text(message)
            }
        case .loaded where offsets.isEmpty:
            ContentUnavailableView {
                Label("No Committed Offsets", systemImage: "arrow.down.to.line")
            } description: {
                Text("This group has never committed an offset.")
            }
        case .loaded, .notLoaded:
            EmptyView()
        }
    }

    private var memberTable: some View {
        Table(group.members) {
            TableColumn("Client ID") { Text($0.clientId).lineLimit(1) }
                .width(min: 160, ideal: 260)
            TableColumn("Host") { Text($0.clientHost).lineLimit(1) }
                .width(min: 100, ideal: 130)
            TableColumn("Topics") { member in
                Text(member.topics.isEmpty ? "—" : member.topics.joined(separator: ", "))
                    .lineLimit(1)
            }
            .width(min: 160, ideal: 280)
            TableColumn("Partitions") { member in
                Text("\(member.partitionCount)").monospacedDigit()
            }
            .width(min: 70, ideal: 90)
        }
        .overlay {
            if group.members.isEmpty {
                ContentUnavailableView {
                    Label("No Members", systemImage: "person.2.slash")
                } description: {
                    Text("The group is not being consumed right now.")
                }
            }
        }
    }
}

/// Moves a group's committed offsets, behind a confirmation.
struct ResetOffsetsSheet: View {
    let clusterID: UUID
    let group: ConsumerGroupInfo
    let offsets: [GroupPartitionOffset]

    @Environment(ClusterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    @State private var mode = Mode.earliest
    @State private var offsetText = "0"
    @State private var scope = Scope.allPartitions
    @State private var selectedTopic: String?
    @State private var isConfirming = false
    @State private var isApplying = false
    @State private var failure: String?

    private enum Mode: String, CaseIterable, Identifiable {
        case earliest = "Earliest"
        case latest = "Latest"
        case numeric = "Offset"

        var id: String { rawValue }
    }

    private enum Scope: String, CaseIterable, Identifiable {
        case allPartitions = "All partitions"
        case oneTopic = "One topic"

        var id: String { rawValue }
    }

    private var topics: [String] { Array(Set(offsets.map(\.topic))).sorted() }

    private var affected: [GroupPartitionOffset] {
        switch scope {
        case .allPartitions: offsets
        case .oneTopic: offsets.filter { $0.topic == (selectedTopic ?? topics.first) }
        }
    }

    private var target: OffsetReset {
        switch mode {
        case .earliest: .earliest
        case .latest: .latest
        case .numeric: .offset(Int64(offsetText) ?? 0)
        }
    }

    /// Kafka refuses to move offsets while the group is being consumed.
    private var hasLiveMembers: Bool { !group.members.isEmpty }

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            VStack(alignment: .leading, spacing: 2) {
                Text("Reset Offsets")
                    .font(.headline)
                Text(group.id)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            if hasLiveMembers {
                Label(
                    """
                    This group has \(group.members.count) active member\(group.members.count == 1 ? "" : "s"). \
                    Kafka will refuse to move its offsets until those consumers stop.
                    """,
                    systemImage: "exclamationmark.triangle"
                )
                .font(.callout)
                .foregroundStyle(.orange)
            }

            Picker("Move to", selection: $mode) {
                ForEach(Mode.allCases) { Text($0.rawValue).tag($0) }
            }
            .pickerStyle(.segmented)

            if mode == .numeric {
                TextField("Offset", text: $offsetText)
                    .frame(width: 140)
                    .monospacedDigit()
            }

            Picker("Apply to", selection: $scope) {
                ForEach(Scope.allCases) { Text($0.rawValue).tag($0) }
            }
            .frame(width: 280)

            if scope == .oneTopic {
                Picker("Topic", selection: $selectedTopic) {
                    ForEach(topics, id: \.self) { Text($0).tag(Optional($0)) }
                }
                .frame(maxWidth: 420)
            }

            Text("\(affected.count) partition\(affected.count == 1 ? "" : "s") will be moved.")
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
                Button("Reset…") { isConfirming = true }
                    .buttonStyle(.borderedProminent)
                    .disabled(isApplying || affected.isEmpty)
            }
        }
        .padding(16)
        .frame(width: 520, height: 400)
        .confirmationDialog(
            "Move \(affected.count) partition\(affected.count == 1 ? "" : "s") of \(group.id)?",
            isPresented: $isConfirming,
            titleVisibility: .visible
        ) {
            Button("Reset Offsets", role: .destructive) { apply() }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text(
                """
                Consumers in this group will resume from the new position. \
                Records between the old and new offsets are either skipped or reprocessed.
                """
            )
        }
    }

    private func apply() {
        guard let connection = store.connection(for: clusterID) else { return }
        isApplying = true
        failure = nil

        Task {
            do {
                try await connection.resetOffsets(
                    group: group.id,
                    positions: affected.map { ($0.topic, $0.partition, target) }
                )
                store.toast = "Reset \(affected.count) partition(s) of \(group.id) to \(mode.rawValue.lowercased())"
                dismiss()
            } catch {
                failure = ClusterConnection.message(for: error)
            }
            isApplying = false
        }
    }
}
