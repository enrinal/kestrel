import AppKit
import KestrelKit
import SwiftUI

/// Which cluster an import is writing to.
struct ImportTarget: Identifiable, Hashable {
    let clusterID: ClusterProfile.ID
    /// Topic to preselect, when the import was started from one.
    let topic: String?

    var id: ClusterProfile.ID { clusterID }
}

/// Reads a file of records and produces them to a chosen topic.
///
/// The file is read as soon as it is chosen, so the count of records, the
/// format it recognised and any unreadable lines are all visible before
/// anything is sent to the broker.
struct ImportSheet: View {
    let target: ImportTarget

    @Environment(ClusterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    @State private var fileURL: URL?
    @State private var plan: ImportPlan?
    @State private var readFailure: String?
    @State private var topic: String = ""
    @State private var isImporting = false
    @State private var progress: (done: Int, total: Int) = (0, 0)
    @State private var outcome: ImportOutcome?

    private var connection: ClusterConnection? {
        store.connection(for: target.clusterID)
    }

    private var topics: [String] {
        (connection?.topics ?? [])
            .map(\.name)
            .filter { store.showsInternalTopics || !$0.hasPrefix("__") }
            .sorted()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Import Records")
                .font(.headline)

            fileRow
            if let plan { summary(of: plan) }
            if let readFailure {
                Label(readFailure, systemImage: "exclamationmark.triangle")
                    .foregroundStyle(.orange)
            }

            Picker("Topic", selection: $topic) {
                Text("Choose a topic…").tag("")
                ForEach(topics, id: \.self) { Text($0).tag($0) }
            }
            .disabled(isImporting)

            if isImporting {
                ProgressView(value: Double(progress.done), total: Double(max(progress.total, 1))) {
                    Text("Producing \(progress.done) of \(progress.total)…")
                }
            }

            if let outcome { results(of: outcome) }

            Spacer(minLength: 0)
            footer
        }
        .padding(20)
        .frame(width: 520, height: 460)
        .onAppear {
            topic = target.topic ?? ""
            if let file = Snapshot.importFile {
                load(file)
                if let snapshotTopic = Snapshot.importTopic { topic = snapshotTopic }
                if Snapshot.runsImport { Task { await runImport() } }
            }
        }
    }

    private var fileRow: some View {
        HStack {
            Button("Choose File…") { chooseFile() }
                .disabled(isImporting)
            Text(fileURL?.lastPathComponent ?? "No file chosen")
                .foregroundStyle(fileURL == nil ? .secondary : .primary)
                .lineLimit(1)
                .truncationMode(.middle)
        }
    }

    private func summary(of plan: ImportPlan) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text("\(plan.records.count) record\(plan.records.count == 1 ? "" : "s") · \(describe(plan.kind))")
                .font(.callout)
            if plan.blankLines > 0 {
                Text("\(plan.blankLines) blank line\(plan.blankLines == 1 ? "" : "s") skipped")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            if !plan.problems.isEmpty {
                problemList(plan.problems, title: "\(plan.problems.count) line(s) cannot be read")
            }
        }
    }

    private func results(of outcome: ImportOutcome) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Label(
                "Produced \(outcome.produced) record\(outcome.produced == 1 ? "" : "s")",
                systemImage: outcome.problems.isEmpty ? "checkmark.circle" : "exclamationmark.circle"
            )
            .foregroundStyle(outcome.problems.isEmpty ? .green : .orange)

            // Only the broker's refusals: the unreadable lines are already
            // listed above, from when the file was read.
            if !outcome.produceProblems.isEmpty {
                problemList(
                    outcome.produceProblems,
                    title: "\(outcome.produceProblems.count) record(s) refused by the broker"
                )
            }
        }
    }

    /// The per-line error list, scrollable because a bad file can have many.
    private func problemList(_ problems: [ImportProblem], title: String) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            ScrollView {
                VStack(alignment: .leading, spacing: 2) {
                    ForEach(problems) { problem in
                        Text("Line \(problem.line): \(problem.message)")
                            .font(.system(.caption, design: .monospaced))
                            .textSelection(.enabled)
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }
            }
            .frame(maxHeight: 110)
        }
    }

    private var footer: some View {
        HStack {
            if let fileURL {
                Text(fileURL.deletingLastPathComponent().path)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .truncationMode(.head)
            }
            Spacer()
            Button("Close") { dismiss() }
                .keyboardShortcut(.cancelAction)
            Button("Import") { Task { await runImport() } }
                .keyboardShortcut(.defaultAction)
                .disabled(!canImport)
        }
    }

    private var canImport: Bool {
        !isImporting && !topic.isEmpty && (plan?.records.isEmpty == false)
    }

    private func describe(_ kind: ImportSourceKind) -> String {
        switch kind {
        case .envelopes: "saved envelopes"
        case .jsonLines: "JSON lines, each taken as a value"
        case .mixed: "envelopes and plain JSON documents"
        }
    }

    private func chooseFile() {
        let panel = NSOpenPanel()
        panel.title = "Choose Records to Import"
        panel.allowsMultipleSelection = false
        panel.canChooseDirectories = false
        guard panel.runModal() == .OK, let url = panel.url else { return }

        load(url)
    }

    /// Reads a file and describes it, without producing anything yet.
    private func load(_ url: URL) {
        fileURL = url
        outcome = nil
        readFailure = nil
        do {
            plan = try RecordImporter.plan(fileURL: url)
        } catch {
            plan = nil
            readFailure = "Could not read the file: \(error.localizedDescription)"
        }
    }

    private func runImport() async {
        guard let plan, let connection else { return }

        isImporting = true
        outcome = nil
        progress = (0, plan.records.count)
        defer { isImporting = false }

        let result = await RecordImporter.run(
            plan: plan,
            progress: { done, total in progress = (done, total) }
        ) { record in
            try await connection.produce(
                topic: topic,
                partition: record.partition,
                key: record.key,
                value: record.value,
                headers: record.headers
            )
        }

        outcome = result
        if Snapshot.isRequested {
            print("DUMP_IMPORT file=\(fileURL?.lastPathComponent ?? "none") kind=\(plan.kind.rawValue) planned=\(plan.records.count)")
            print("DUMP_IMPORT produced=\(result.produced) problems=\(result.problems.count) topic=\(topic)")
            print("DUMP_IMPORT offsets=\(result.reports.map(\.offset))")
            for problem in result.problems {
                print("DUMP_IMPORT_PROBLEM line=\(problem.line) \(problem.message)")
            }
        }
        if result.produced > 0 {
            store.toast = "Imported \(result.produced) record\(result.produced == 1 ? "" : "s") into \(topic)"
            // The topic's watermarks and any open record page are now stale.
            await connection.refresh()
        }
    }
}
