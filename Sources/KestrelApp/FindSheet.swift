import KestrelKit
import SwiftUI

/// What a search is scanning.
struct FindTarget: Identifiable, Hashable {
    let clusterID: ClusterProfile.ID
    /// Topic to scope to, when the search was started from one.
    let topic: String?

    var id: ClusterProfile.ID { clusterID }
}

/// Which records a search reads.
private enum FindScope: String, CaseIterable, Identifiable {
    case topic = "This topic"
    case cluster = "Whole cluster"

    var id: String { rawValue }
}

/// Where the browser should jump to after a hit is chosen.
struct RecordJump: Equatable {
    let topic: String
    let partition: Int32
    let offset: Int64
}

/// Searches records for a string, and opens the ones that match.
struct FindSheet: View {
    let target: FindTarget

    @Environment(ClusterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    @State private var text = ""
    @State private var isRegex = false
    @State private var isCaseSensitive = false
    @State private var searchesKeys = true
    @State private var searchesValues = true
    @State private var scope: FindScope = .topic
    @State private var hits: [SearchHit] = []
    @State private var selected: SearchHit.ID?
    @State private var status: String?
    @State private var failure: String?
    @State private var scan: Task<Void, Never>?

    private var connection: ClusterConnection? {
        store.connection(for: target.clusterID)
    }

    private var isScanning: Bool { scan != nil }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Find Messages")
                .font(.headline)

            TextField("Text to find", text: $text)
                .font(.system(.body, design: .monospaced))
                .onSubmit { start() }
                .disabled(isScanning)

            HStack(spacing: 16) {
                Toggle("Regex", isOn: $isRegex)
                Toggle("Match case", isOn: $isCaseSensitive)
                Toggle("Keys", isOn: $searchesKeys)
                Toggle("Values", isOn: $searchesValues)
            }
            .disabled(isScanning)

            Picker("Scope", selection: $scope) {
                ForEach(FindScope.allCases) { option in
                    // Searching one topic is only on offer when one is chosen.
                    if option == .topic, target.topic == nil {
                        EmptyView()
                    } else {
                        Text(option == .topic ? "Topic: \(target.topic ?? "")" : option.rawValue)
                            .tag(option)
                    }
                }
            }
            .disabled(isScanning)

            if let status {
                Text(status).font(.callout).foregroundStyle(.secondary)
            }
            if let failure {
                Label(failure, systemImage: "exclamationmark.triangle")
                    .font(.callout)
                    .foregroundStyle(.orange)
            }

            results
            footer
        }
        .padding(20)
        .frame(width: 640, height: 520)
        .onAppear {
            if target.topic == nil { scope = .cluster }
            if Snapshot.isRequested { applySnapshotSettings() }
            if Snapshot.runsFind { start() }
        }
        .onDisappear { scan?.cancel() }
    }

    private var results: some View {
        Table(hits, selection: $selected) {
            TableColumn("Topic") { Text($0.topic).lineLimit(1).truncationMode(.head) }
            TableColumn("Partition") { Text("\($0.partition)") }.width(70)
            TableColumn("Offset") { Text("\($0.offset)") }.width(90)
            TableColumn("In") { Text($0.field.rawValue) }.width(50)
            TableColumn("Match") {
                Text($0.excerpt)
                    .font(.system(.caption, design: .monospaced))
                    .lineLimit(1)
            }
        }
        .frame(minHeight: 220)
        // Double-click is the usual way to open a result; the Go button does
        // the same for anyone who prefers the keyboard.
        .contextMenu(forSelectionType: SearchHit.ID.self) { _ in
        } primaryAction: { ids in
            if let id = ids.first { open(id) }
        }
    }

    private var footer: some View {
        HStack {
            Spacer()
            Button("Close") { dismiss() }
                .keyboardShortcut(.cancelAction)
            if isScanning {
                Button("Stop") { scan?.cancel() }
            } else {
                Button("Find") { start() }
                    .keyboardShortcut(.defaultAction)
                    .disabled(!canSearch)
            }
            Button("Go to Message") { if let selected { open(selected) } }
                .disabled(selected == nil)
        }
    }

    private var canSearch: Bool {
        !text.isEmpty && (searchesKeys || searchesValues)
    }

    /// Partitions to scan, from the chosen scope.
    private func scopes() -> [SearchScope] {
        guard let connection else { return [] }
        let topics = switch scope {
        case .topic: connection.topics.filter { $0.name == target.topic }
        case .cluster: connection.topics.filter { store.showsInternalTopics || !$0.name.hasPrefix("__") }
        }
        return topics.map { SearchScope(topic: $0.name, partitions: $0.partitions.map(\.id).sorted()) }
    }

    private func start() {
        guard let connection, canSearch else { return }

        hits = []
        selected = nil
        failure = nil
        status = "Scanning…"

        let query = SearchQuery(
            text: text,
            isRegex: isRegex,
            searchesKeys: searchesKeys,
            searchesValues: searchesValues,
            isCaseSensitive: isCaseSensitive
        )
        let targets = scopes()

        scan = Task {
            defer { scan = nil }
            do {
                let outcome = try await connection.search(
                    query: query,
                    scopes: targets,
                    progress: { scanned, found in
                        Task { @MainActor in status = "Scanned \(scanned) records, \(found) hit(s)…" }
                    }
                )
                hits = outcome.hits
                status = describe(outcome)
                if Snapshot.opensFirstHit, let first = outcome.hits.first {
                    open(first.id)
                }
                if Snapshot.isRequested {
                    print("DUMP_FIND query=\(query.text) regex=\(query.isRegex) scanned=\(outcome.scanned) hits=\(outcome.hits.count) cancelled=\(outcome.wasCancelled)")
                    for hit in outcome.hits.prefix(5) {
                        print("DUMP_FIND_HIT \(hit.topic) p\(hit.partition) offset=\(hit.offset) in=\(hit.field.rawValue) \(hit.excerpt.prefix(70))")
                    }
                }
            } catch {
                status = nil
                failure = (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
                if Snapshot.isRequested { print("DUMP_FIND_FAILED \(failure ?? "")") }
            }
        }
    }

    private func describe(_ outcome: SearchOutcome) -> String {
        var parts = ["\(outcome.hits.count) hit(s) in \(outcome.scanned) records"]
        if outcome.reachedLimit { parts.append("stopped at the hit limit") }
        if outcome.wasCancelled { parts.append("stopped early") }
        return parts.joined(separator: " · ")
    }

    /// Opens a hit in the message browser, at its own offset.
    private func open(_ id: SearchHit.ID) {
        guard let hit = hits.first(where: { $0.id == id }) else { return }

        store.selection = .topic(target.clusterID, hit.topic)
        store.topicPane = .messages
        store.recordJump = RecordJump(
            topic: hit.topic,
            partition: hit.partition,
            offset: hit.offset
        )
        if Snapshot.isRequested {
            print("DUMP_FIND_OPEN \(hit.topic) p\(hit.partition) offset=\(hit.offset)")
        }
        dismiss()
    }

    /// Fills the sheet from the snapshot environment for a headless run.
    private func applySnapshotSettings() {
        if let query = Snapshot.findQuery { text = query }
        if Snapshot.findScopeIsCluster { scope = .cluster }
    }
}
