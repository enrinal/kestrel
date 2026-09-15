import AppKit
import KestrelKit
import SwiftUI

/// What an export is reading from.
struct ExportTarget: Identifiable, Hashable {
    let clusterID: ClusterProfile.ID
    /// Topic to preselect, when the export was started from one.
    let topic: String?

    var id: ClusterProfile.ID { clusterID }
}

/// Where an export writes.
private enum ExportDestination: String, CaseIterable, Identifiable {
    case file = "JSONL File"
    case topic = "Another Topic"

    var id: String { rawValue }
}

/// Reads a range of records out of a topic, into a file or another topic.
///
/// The record count is worked out from the partitions' watermarks before
/// anything is read, so the range on screen is the range that will be written.
struct ExportSheet: View {
    let target: ExportTarget

    @Environment(ClusterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    @State private var topic: String = ""
    /// Empty means every partition.
    @State private var partition: String = ""
    @State private var startOffset: String = ""
    @State private var endOffset: String = ""
    @State private var destination: ExportDestination = .file
    @State private var destinationTopic: String = ""
    @State private var estimate: Int?
    @State private var isExporting = false
    @State private var progress: (done: Int, total: Int) = (0, 0)
    @State private var summary: String?
    @State private var failure: String?

    private var connection: ClusterConnection? {
        store.connection(for: target.clusterID)
    }

    private var topics: [String] {
        (connection?.topics ?? [])
            .map(\.name)
            .filter { store.showsInternalTopics || !$0.hasPrefix("__") }
            .sorted()
    }

    /// Partitions of the chosen topic, from metadata already loaded.
    private var partitionIDs: [Int32] {
        (connection?.topic(named: topic)?.partitions ?? []).map(\.id).sorted()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Export Records")
                .font(.headline)

            Picker("Topic", selection: $topic) {
                Text("Choose a topic…").tag("")
                ForEach(topics, id: \.self) { Text($0).tag($0) }
            }
            .disabled(isExporting)

            Picker("Partition", selection: $partition) {
                Text("All").tag("")
                ForEach(partitionIDs, id: \.self) { Text("\($0)").tag(String($0)) }
            }
            .disabled(isExporting)

            HStack {
                LabeledContent("From offset") {
                    TextField("earliest", text: $startOffset)
                        .frame(width: 90)
                }
                LabeledContent("To offset") {
                    TextField("latest", text: $endOffset)
                        .frame(width: 90)
                }
            }
            Text("The end offset is exclusive, matching how Kafka reports a partition's end.")
                .font(.caption)
                .foregroundStyle(.secondary)

            Picker("Write to", selection: $destination) {
                ForEach(ExportDestination.allCases) { Text($0.rawValue).tag($0) }
            }
            .pickerStyle(.radioGroup)
            .disabled(isExporting)

            if destination == .topic {
                Picker("Destination", selection: $destinationTopic) {
                    Text("Choose a topic…").tag("")
                    ForEach(topics.filter { $0 != topic }, id: \.self) { Text($0).tag($0) }
                }
                .disabled(isExporting)
            }

            if let estimate {
                Text("\(estimate) record\(estimate == 1 ? "" : "s") in range")
                    .font(.callout)
            }

            if isExporting {
                ProgressView(value: Double(progress.done), total: Double(max(progress.total, 1))) {
                    Text("Exported \(progress.done) of \(progress.total)…")
                }
            }

            if let summary {
                Label(summary, systemImage: "checkmark.circle")
                    .foregroundStyle(.green)
            }
            if let failure {
                Label(failure, systemImage: "exclamationmark.triangle")
                    .foregroundStyle(.orange)
            }

            Spacer(minLength: 0)
            footer
        }
        .padding(20)
        .frame(width: 520, height: 470)
        .onAppear {
            topic = target.topic ?? ""
            if Snapshot.isRequested { applySnapshotSettings() }
            Task {
                await refreshEstimate()
                if Snapshot.runsExport { await run() }
            }
        }
        .onChange(of: topic) { _, _ in
            partition = ""
            Task { await refreshEstimate() }
        }
        .onChange(of: partition) { _, _ in Task { await refreshEstimate() } }
        .onChange(of: startOffset) { _, _ in Task { await refreshEstimate() } }
        .onChange(of: endOffset) { _, _ in Task { await refreshEstimate() } }
    }

    private var footer: some View {
        HStack {
            Spacer()
            Button("Close") { dismiss() }
                .keyboardShortcut(.cancelAction)
            Button("Export") { Task { await run() } }
                .keyboardShortcut(.defaultAction)
                .disabled(!canExport)
        }
    }

    private var canExport: Bool {
        guard !isExporting, !topic.isEmpty, (estimate ?? 0) > 0 else { return false }
        return destination == .file || !destinationTopic.isEmpty
    }

    private func request() -> ExportRequest {
        ExportRequest(
            topic: topic,
            partitions: partition.isEmpty ? partitionIDs : [Int32(partition) ?? 0],
            startOffset: Int64(startOffset),
            endOffset: Int64(endOffset)
        )
    }

    /// Counts what the current range covers, so the button and the count agree.
    private func refreshEstimate() async {
        guard !topic.isEmpty, let connection else {
            estimate = nil
            return
        }
        do {
            estimate = try await connection.countForExport(request: request())
            failure = nil
        } catch {
            estimate = nil
            failure = "Could not read the range: \(error.localizedDescription)"
        }
    }

    private func run() async {
        guard let connection else { return }

        summary = nil
        failure = nil

        let sink: ExportSink
        var fileURL: URL?
        switch destination {
        case .file:
            guard let url = chooseFile() else { return }
            fileURL = url
            do {
                sink = try JSONLFileSink(url: url)
            } catch {
                failure = "Could not create the file: \(error.localizedDescription)"
                return
            }
        case .topic:
            let destinationName = destinationTopic
            sink = TopicSink { [connection] record in
                try await connection.produce(
                    topic: destinationName,
                    partition: nil,
                    key: record.key,
                    value: record.value,
                    headers: record.headers
                )
            }
        }

        isExporting = true
        progress = (0, estimate ?? 0)
        defer { isExporting = false }

        do {
            let outcome = try await connection.export(
                request: request(),
                sink: sink,
                progress: { done, total in progress = (done, total) }
            )
            let where_ = fileURL?.lastPathComponent ?? destinationTopic
            summary = "Exported \(outcome.records) record\(outcome.records == 1 ? "" : "s") to \(where_)"
            store.toast = summary
            if destination == .topic { await connection.refresh() }

            if Snapshot.isRequested {
                print("DUMP_EXPORT topic=\(topic) partitions=\(request().partitions) records=\(outcome.records) bytes=\(outcome.bytesWritten)")
                print("DUMP_EXPORT byPartition=\(outcome.byPartition.sorted { $0.key < $1.key }) destination=\(where_)")
            }
        } catch {
            failure = "Export failed: \(error.localizedDescription)"
            if Snapshot.isRequested { print("DUMP_EXPORT_FAILED \(error.localizedDescription)") }
        }
    }

    /// Asks where to write, or takes the snapshot's path when running headless.
    private func chooseFile() -> URL? {
        if let file = Snapshot.exportFile { return file }

        let panel = NSSavePanel()
        panel.title = "Export Records"
        panel.nameFieldStringValue = "\(topic).jsonl"
        panel.canCreateDirectories = true
        guard panel.runModal() == .OK else { return nil }
        return panel.url
    }

    /// Fills the sheet from the snapshot environment, so a headless run can
    /// exercise it without a save panel or any typing.
    private func applySnapshotSettings() {
        if let snapshotTopic = Snapshot.exportTopic { topic = snapshotTopic }
        if let range = Snapshot.exportRange {
            startOffset = range.start
            endOffset = range.end
        }
        if let destinationName = Snapshot.exportToTopic {
            destination = .topic
            destinationTopic = destinationName
        }
    }
}
