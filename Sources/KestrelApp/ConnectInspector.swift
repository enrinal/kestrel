import KestrelKit
import SwiftUI

/// The connector list for a cluster, shown when the Connect folder is selected.
struct ConnectFolderInspector: View {
    let clusterID: UUID

    @Environment(ClusterStore.self) private var store

    private var connection: ClusterConnection? { store.connection(for: clusterID) }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Label("Kafka Connect", systemImage: "point.3.connected.trianglepath.dotted")
                    .font(.title2)
                Spacer()
                Button("Refresh", systemImage: "arrow.clockwise") {
                    Task { await connection?.loadConnectors(force: true) }
                }
            }

            Text(connection?.profile.connect.url ?? "")
                .font(.caption)
                .foregroundStyle(.secondary)
                .textSelection(.enabled)

            switch connection?.connectorsLoad {
            case .loading:
                HStack(spacing: 6) {
                    ProgressView().controlSize(.small)
                    Text("Listing connectors…").foregroundStyle(.secondary)
                }

            case .failed(let message):
                // The acceptance line for this slice: an unreachable worker is
                // explained, rather than showing an empty list that looks like
                // a Connect cluster with nothing deployed.
                ConnectProblem(message: message) {
                    Task { await connection?.loadConnectors(force: true) }
                }

            default:
                let connectors = connection?.connectors ?? []
                if connectors.isEmpty {
                    Text("This Connect cluster has no connectors deployed.")
                        .foregroundStyle(.secondary)
                } else {
                    Table(connectors) {
                        // Wide: connector names are long and reversed-domain,
                        // and truncating the middle hides which one it is.
                        TableColumn("Connector") { Text($0.name).monospaced() }
                            .width(min: 220, ideal: 280)
                        TableColumn("Type") { Text($0.kind.rawValue.capitalized) }
                            .width(60)
                        TableColumn("State") { ConnectorStateBadge(state: $0.state) }
                            .width(100)
                        TableColumn("Tasks") { connector in
                            Text(taskSummary(connector))
                                .foregroundStyle(connector.needsAttention ? .orange : .primary)
                        }
                        TableColumn("Class") {
                            Text($0.connectorClass?.components(separatedBy: ".").last ?? "—")
                                .font(.callout)
                        }
                    }
                }
            }

            Spacer()
        }
        .padding(16)
        .task { await connection?.loadConnectors() }
    }

    private func taskSummary(_ connector: ConnectorInfo) -> String {
        let running = connector.tasks.filter { $0.state == .running }.count
        return "\(running)/\(connector.tasks.count) running"
    }
}

/// One connector: status, tasks, configuration, and the actions.
struct ConnectorInspector: View {
    let clusterID: UUID
    let connector: ConnectorInfo

    @Environment(ClusterStore.self) private var store
    @State private var failure: String?
    @State private var confirmingRestart = false

    private var connection: ClusterConnection? { store.connection(for: clusterID) }
    private var isBusy: Bool {
        connection?.busyConnectors.contains(connector.name) ?? false
    }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 16) {
                header
                actions

                if let failure {
                    Label(failure, systemImage: "exclamationmark.triangle.fill")
                        .foregroundStyle(.orange)
                        .font(.callout)
                        .textSelection(.enabled)
                }

                if let trace = connector.trace {
                    TraceBlock(title: "Connector failure", trace: trace)
                }

                tasks
                configuration
            }
            .padding(16)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .navigationTitle(connector.name)
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 8) {
                Label(connector.name, systemImage: connector.kind == .sink ? "tray.and.arrow.down" : "tray.and.arrow.up")
                    .font(.title2)
                ConnectorStateBadge(state: connector.state)
                if isBusy {
                    ProgressView().controlSize(.small)
                }
            }

            HStack(alignment: .top, spacing: 24) {
                InspectorField("Type", connector.kind.rawValue.capitalized)
                InspectorField("Worker", connector.workerID.isEmpty ? "—" : connector.workerID)
                InspectorField("Tasks", "\(connector.tasks.count)")
                InspectorField(
                    "Class",
                    connector.connectorClass?.components(separatedBy: ".").last ?? "—"
                )
            }
        }
    }

    private var actions: some View {
        HStack {
            Button("Pause", systemImage: "pause.fill") { run(.pause) }
                .disabled(isBusy || connector.state == .paused)
            Button("Resume", systemImage: "play.fill") { run(.resume) }
                .disabled(isBusy || connector.state == .running)
            Button("Restart…", systemImage: "arrow.clockwise") { confirmingRestart = true }
                .disabled(isBusy)
            Spacer()
            Button("Refresh", systemImage: "arrow.triangle.2.circlepath") {
                Task { await connection?.loadConnectors(force: true) }
            }
        }
        // Restarting interrupts whatever the connector is moving, so it asks
        // first; pause and resume are reversible and do not.
        .confirmationDialog(
            "Restart \(connector.name)?",
            isPresented: $confirmingRestart,
            titleVisibility: .visible
        ) {
            Button("Restart Connector and Tasks", role: .destructive) { run(.restart) }
        } message: {
            Text("The connector and its \(connector.tasks.count) task(s) will stop and start again.")
        }
    }

    @ViewBuilder
    private var tasks: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Tasks")
                .font(.caption)
                .foregroundStyle(.secondary)

            if connector.tasks.isEmpty {
                Text("This connector has no tasks. A paused or freshly created connector has none until the worker assigns them.")
                    .font(.callout)
                    .foregroundStyle(.secondary)
            } else {
                ForEach(connector.tasks) { task in
                    VStack(alignment: .leading, spacing: 4) {
                        HStack(spacing: 8) {
                            Text("Task \(task.id)").monospaced().bold()
                            ConnectorStateBadge(state: task.state)
                            Text(task.workerID)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            Spacer()
                            Button("Restart Task") {
                                restart(task: task.id)
                            }
                            .buttonStyle(.borderless)
                            .disabled(isBusy)
                        }
                        if let trace = task.trace {
                            TraceBlock(title: "Task \(task.id) failure", trace: trace)
                        }
                    }
                    .font(.callout)
                    .padding(8)
                    .background(.quaternary.opacity(0.5), in: RoundedRectangle(cornerRadius: 6))
                }
            }
        }
    }

    private var configuration: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Configuration (\(connector.config.count))")
                .font(.caption)
                .foregroundStyle(.secondary)

            ForEach(connector.config.keys.sorted(), id: \.self) { key in
                HStack(alignment: .top) {
                    Text(key)
                        .monospaced()
                        .frame(width: 260, alignment: .leading)
                    Text(connector.config[key] ?? "")
                        .monospaced()
                        .textSelection(.enabled)
                }
                .font(.callout)
            }
        }
    }

    private func run(_ action: ClusterConnection.ConnectorAction) {
        failure = nil
        Task { failure = await connection?.run(action, on: connector.name) }
    }

    private func restart(task: Int) {
        failure = nil
        Task { failure = await connection?.restartTask(task, of: connector.name) }
    }
}

/// A connector or task state, coloured by whether it needs attention.
struct ConnectorStateBadge: View {
    let state: ConnectorState

    var body: some View {
        Text(state.label)
            .font(.caption2)
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .background(tint.opacity(0.18), in: Capsule())
            .foregroundStyle(tint)
    }

    private var tint: Color {
        switch state {
        case .running: .green
        case .paused, .stopped: .orange
        case .failed, .unassigned: .red
        default: .secondary
        }
    }
}

/// A Connect failure with a retry, used where a list would otherwise be empty.
private struct ConnectProblem: View {
    let message: String
    let retry: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Label(message, systemImage: "exclamationmark.triangle.fill")
                .foregroundStyle(.orange)
                .textSelection(.enabled)
            Button("Try Again", systemImage: "arrow.clockwise", action: retry)
        }
        .padding(12)
        .background(.quaternary.opacity(0.5), in: RoundedRectangle(cornerRadius: 8))
    }
}

/// A stack trace, trimmed to its first lines with the rest available on scroll.
private struct TraceBlock: View {
    let title: String
    let trace: String

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.orange)
            ScrollView(.horizontal) {
                Text(trace)
                    .monospaced()
                    .font(.caption)
                    .textSelection(.enabled)
            }
            .frame(maxHeight: 160)
            .padding(8)
            .background(.quaternary, in: RoundedRectangle(cornerRadius: 6))
        }
    }
}
