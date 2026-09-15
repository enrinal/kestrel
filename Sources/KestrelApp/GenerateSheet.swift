import KestrelKit
import SwiftUI

/// What a generation run is writing to.
struct GenerateTarget: Identifiable, Hashable {
    let clusterID: ClusterProfile.ID
    /// Topic to preselect, when started from one.
    let topic: String?

    var id: ClusterProfile.ID { clusterID }
}

/// Ready-made templates, so the sheet is useful without reading the
/// placeholder reference first.
private enum GeneratePreset: String, CaseIterable, Identifiable {
    case json = "JSON object"
    case text = "Lorem text"
    case number = "Just the index"

    var id: String { rawValue }

    var keyTemplate: String {
        switch self {
        case .json: "{{uuid}}"
        case .text: "key-{{index}}"
        case .number: ""
        }
    }

    var valueTemplate: String {
        switch self {
        case .json:
            #"{"id":"{{uuid}}","index":{{index}},"name":"{{lorem:3}}","score":{{int:1-100}},"at":"{{timestamp}}"}"#
        case .text:
            "{{lorem:12}}"
        case .number:
            "{{index}}"
        }
    }
}

/// Fills a topic with generated records.
struct GenerateSheet: View {
    let target: GenerateTarget

    @Environment(ClusterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    @State private var topic: String = ""
    @State private var count: String = "100"
    @State private var keyTemplate: String = GeneratePreset.json.keyTemplate
    @State private var valueTemplate: String = GeneratePreset.json.valueTemplate
    @State private var isGenerating = false
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

    /// Placeholders in either template that would not be filled in.
    private var unknown: [String] {
        let inKey = TemplateExpander.unknownPlaceholders(in: keyTemplate)
        let inValue = TemplateExpander.unknownPlaceholders(in: valueTemplate)
        return inKey + inValue.filter { !inKey.contains($0) }
    }

    private var requestedCount: Int { Int(count) ?? 0 }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Generate Records")
                .font(.headline)

            Picker("Topic", selection: $topic) {
                Text("Choose a topic…").tag("")
                ForEach(topics, id: \.self) { Text($0).tag($0) }
            }
            .disabled(isGenerating)

            HStack {
                LabeledContent("Count") {
                    TextField("100", text: $count)
                        .frame(width: 80)
                }
                Spacer()
                Menu("Preset") {
                    ForEach(GeneratePreset.allCases) { preset in
                        Button(preset.rawValue) {
                            keyTemplate = preset.keyTemplate
                            valueTemplate = preset.valueTemplate
                        }
                    }
                }
                .fixedSize()
            }
            .disabled(isGenerating)

            templateField("Key template", text: $keyTemplate, prompt: "leave empty for no key")
            templateField("Value template", text: $valueTemplate, prompt: "record value")

            if !unknown.isEmpty {
                Label(
                    "Not a placeholder: \(unknown.map { "{{\($0)}}" }.joined(separator: ", ")) — it will be sent as written",
                    systemImage: "exclamationmark.triangle"
                )
                .font(.caption)
                .foregroundStyle(.orange)
            }

            preview

            if isGenerating {
                ProgressView(value: Double(progress.done), total: Double(max(progress.total, 1))) {
                    Text("Produced \(progress.done) of \(progress.total)…")
                }
            }
            if let summary {
                Label(summary, systemImage: "checkmark.circle").foregroundStyle(.green)
            }
            if let failure {
                Label(failure, systemImage: "exclamationmark.triangle").foregroundStyle(.orange)
            }

            Spacer(minLength: 0)
            footer
        }
        .padding(20)
        .frame(width: 560, height: 520)
        .onAppear {
            topic = target.topic ?? ""
            if Snapshot.isRequested { applySnapshotSettings() }
            if Snapshot.runsGenerate { Task { await run() } }
        }
    }

    private func templateField(
        _ title: String,
        text: Binding<String>,
        prompt: String
    ) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(title).font(.caption).foregroundStyle(.secondary)
            TextField(prompt, text: text, axis: .vertical)
                .font(.system(.body, design: .monospaced))
                .lineLimit(2...3)
                .disabled(isGenerating)
        }
    }

    /// The first few records this template would produce.
    ///
    /// Seeded, so the preview does not reshuffle on every keystroke.
    private var preview: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text("Preview").font(.caption).foregroundStyle(.secondary)
            ForEach(Array(previewRows.enumerated()), id: \.offset) { _, row in
                Text(row)
                    .font(.system(.caption, design: .monospaced))
                    .lineLimit(1)
                    .truncationMode(.tail)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
    }

    private var previewRows: [String] {
        RecordGenerator.preview(
            request: GenerateRequest(
                topic: topic,
                count: max(requestedCount, 1),
                keyTemplate: keyTemplate,
                valueTemplate: valueTemplate,
                seed: 0
            ),
            limit: 3
        )
        .map { row in
            row.key.map { "\($0) → \(row.value)" } ?? row.value
        }
    }

    private var footer: some View {
        HStack {
            Text("Placeholders: \(TemplateExpander.documentation.map(\.name).joined(separator: " "))")
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(2)
            Spacer()
            Button("Close") { dismiss() }
                .keyboardShortcut(.cancelAction)
            Button("Generate") { Task { await run() } }
                .keyboardShortcut(.defaultAction)
                .disabled(isGenerating || topic.isEmpty || requestedCount < 1 || valueTemplate.isEmpty)
        }
    }

    private func run() async {
        guard let connection else { return }

        summary = nil
        failure = nil
        isGenerating = true
        progress = (0, requestedCount)
        defer { isGenerating = false }

        let outcome = await connection.generate(
            request: GenerateRequest(
                topic: topic,
                count: requestedCount,
                keyTemplate: keyTemplate,
                valueTemplate: valueTemplate
            ),
            progress: { done, total in
                Task { @MainActor in progress = (done, total) }
            }
        )

        summary = "Generated \(outcome.produced) record\(outcome.produced == 1 ? "" : "s") on \(topic)"
        store.toast = summary
        if !outcome.failures.isEmpty {
            failure = "\(outcome.failures.count) record(s) refused: \(outcome.failures.values.first ?? "")"
        }
        await connection.refresh()

        if Snapshot.isRequested {
            print("DUMP_GENERATE topic=\(topic) requested=\(requestedCount) produced=\(outcome.produced) refused=\(outcome.failures.count)")
            print("DUMP_GENERATE firstOffset=\(outcome.offsets.first ?? -1) lastOffset=\(outcome.offsets.last ?? -1)")
        }
    }

    /// Fills the sheet from the snapshot environment for a headless run.
    private func applySnapshotSettings() {
        if let snapshotTopic = Snapshot.generateTopic { topic = snapshotTopic }
        if let snapshotCount = Snapshot.generateCount { count = snapshotCount }
    }
}
