import KestrelKit
import SwiftUI

/// Browses the records of one partition: pick where to start, read a page, and
/// inspect a record.
struct MessageBrowser: View {
    let clusterID: UUID
    let topic: TopicInfo

    @Environment(ClusterStore.self) private var store

    @State private var partition: Int32 = 0
    @State private var start: StartMode = .latest
    @State private var latestCount = 50
    @State private var offsetText = "0"
    @State private var limit = 100

    /// The three ways a read can begin, matching ``StartPosition``.
    private enum StartMode: String, CaseIterable, Identifiable {
        case earliest = "Earliest"
        case latest = "Latest N"
        case offset = "Offset"

        var id: String { rawValue }
    }

    private var connection: ClusterConnection? { store.connection(for: clusterID) }

    private var key: ClusterConnection.PartitionKey {
        .init(topic: topic.name, partition: partition)
    }

    private var page: RecordPage? { connection?.page(for: key) }
    private var load: ClusterConnection.Load { connection?.recordLoad(for: key) ?? .notLoaded }

    private var startPosition: StartPosition {
        switch start {
        case .earliest: .earliest
        case .latest: .latest(count: Int64(latestCount))
        case .offset: .offset(Int64(offsetText) ?? 0)
        }
    }

    private var records: [KafkaRecord] { page?.records ?? [] }

    var body: some View {
        VStack(spacing: 0) {
            controls
            Divider()
            VSplitView {
                recordTable
                    .frame(minHeight: 160)
                RecordDetail(
                    record: records.first { $0.id == store.selectedRecord },
                    topic: topic.name,
                    connection: connection
                )
                .frame(minHeight: 140)
            }
        }
        .onChange(of: topic.name) { resetSelection() }
        .onChange(of: partition) { resetSelection() }
    }

    /// Opens the offset a search hit asked for, and selects that record.
    ///
    /// The request is cleared once honoured, so coming back to this topic
    /// later does not jump again.
    private func honourJump() async {
        guard let jump = store.recordJump, jump.topic == topic.name else { return }
        store.recordJump = nil

        partition = jump.partition
        start = .offset
        offsetText = String(jump.offset)

        await connection?.loadRecords(
            topic: topic.name,
            partition: jump.partition,
            from: .offset(jump.offset),
            limit: max(1, limit)
        )
        // Select the record the hit named, not merely the page it sits in.
        store.selectedRecord = records.first { $0.offset == jump.offset }?.id

        if Snapshot.isRequested {
            print("DUMP_JUMP topic=\(topic.name) partition=\(jump.partition) offset=\(jump.offset) selected=\(store.selectedRecord ?? "none")")
        }
    }

    private func resetSelection() {
        store.selectedRecord = nil
    }

    // MARK: Controls

    @ViewBuilder
    private var controls: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Picker("Partition", selection: $partition) {
                    ForEach(topic.partitions) { Text("\($0.id)").tag($0.id) }
                }
                .frame(width: 140)

                Picker("From", selection: $start) {
                    ForEach(StartMode.allCases) { Text($0.rawValue).tag($0) }
                }
                .frame(width: 160)

                switch start {
                case .latest:
                    TextField("Count", value: $latestCount, format: .number)
                        .frame(width: 70)
                case .offset:
                    TextField("Offset", text: $offsetText)
                        .frame(width: 90)
                        .monospacedDigit()
                case .earliest:
                    EmptyView()
                }

                TextField("Limit", value: $limit, format: .number)
                    .frame(width: 70)

                Button("Load") {
                    Task {
                        await connection?.loadRecords(
                            topic: topic.name,
                            partition: partition,
                            from: startPosition,
                            limit: max(1, limit)
                        )
                    }
                }
                .buttonStyle(.borderedProminent)
                .disabled(load == .loading)

                Spacer()

                Button("Produce…") {
                    store.produceTarget = ProduceTarget(clusterID: clusterID, topic: topic.name)
                }
            }

            if let page {
                Text(summary(for: page))
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .monospacedDigit()
            }
        }
        .task(id: store.recordJump) { await honourJump() }
        .padding(12)
    }

    private func summary(for page: RecordPage) -> String {
        let range = "offsets \(page.lowWatermark)…\(page.highWatermark - 1)"
        if page.isPartitionEmpty {
            return "Partition \(partition) is empty (high watermark \(page.highWatermark))"
        }
        let end = page.reachedEnd ? ", reached end of partition" : ""
        return "\(page.records.count) records from offset \(page.startOffset) · partition holds \(range)\(end)"
    }

    // MARK: Table

    private var recordTable: some View {
        Table(records, selection: Binding(get: { store.selectedRecord }, set: { store.selectedRecord = $0 })) {
            TableColumn("Offset") { Text("\($0.offset)").monospacedDigit() }
                .width(min: 70, ideal: 80)
            TableColumn("Timestamp") { record in
                Text(record.timestamp?.formatted(date: .numeric, time: .standard) ?? "—")
                    .monospacedDigit()
            }
            .width(min: 140, ideal: 170)
            TableColumn("Key") { Text($0.keyPreview).lineLimit(1) }
                .width(min: 100, ideal: 180)
            TableColumn("Value") { Text($0.valuePreview).lineLimit(1) }
                .width(min: 180, ideal: 320)
            TableColumn("Headers") { Text("\($0.headers.count)").monospacedDigit() }
                .width(min: 60, ideal: 70)
            TableColumn("Size") { Text("\($0.valueByteCount) B").monospacedDigit() }
                .width(min: 70, ideal: 80)
        }
        .overlay { tableOverlay }
    }

    @ViewBuilder
    private var tableOverlay: some View {
        switch load {
        case .loading:
            ProgressView("Reading records…")
        case .failed(let message):
            ContentUnavailableView {
                Label("Could Not Read Records", systemImage: "exclamationmark.triangle")
            } description: {
                Text(message)
            }
        case .notLoaded:
            ContentUnavailableView {
                Label("No Records Loaded", systemImage: "tray")
            } description: {
                Text("Pick a start position, then choose Load.")
            }
        case .loaded where records.isEmpty:
            // An empty partition must say so rather than spin forever.
            ContentUnavailableView {
                Label(
                    page?.isPartitionEmpty == true ? "Partition Is Empty" : "No Records In Range",
                    systemImage: "tray"
                )
            } description: {
                if page?.isPartitionEmpty == true {
                    Text("Partition \(partition) of \(topic.name) holds no records.")
                } else {
                    Text("Nothing between the chosen start offset and the end of the partition.")
                }
            }
        case .loaded:
            EmptyView()
        }
    }
}

/// Key, value, headers and metadata of the selected record.
struct RecordDetail: View {
    /// What decoding a Confluent-framed payload produced.
    enum AvroState: Equatable {
        case none
        case decoding
        case decoded(DecodedAvro)
        case failed(String)
    }

    let record: KafkaRecord?
    /// Topic the record came from, recorded in a saved envelope.
    var topic: String = ""
    /// Needed for the cluster's Schema Registry. Without it a framed payload
    /// can still be recognised as Avro, but not decoded.
    var connection: ClusterConnection?

    /// Pretty by default; the toggle is remembered for the session.
    @State private var showsPretty = true
    @State private var avro: AvroState = .none
    /// Shows the schema document rather than the decoded record.
    @State private var showsSchema = false

    private var value: FormattedPayload? { record.map { PayloadFormatter.format($0.value) } }
    private var key: FormattedPayload? { record.map { PayloadFormatter.format($0.key) } }

    var body: some View {
        if let record, let value, let key {
            ScrollView {
                VStack(alignment: .leading, spacing: 12) {
                    metadata(for: record, value: value, key: key)

                    PayloadBlock(title: "Key", payload: key, showsPretty: showsPretty)
                    avroBlock(for: record)

                    PayloadBlock(
                        title: record.isTombstone ? "Value (tombstone)" : "Value",
                        payload: value,
                        showsPretty: showsPretty
                    )

                    headers(of: record)
                }
                .padding(12)
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            .task(id: record.id) { await decodeAvro(record) }
        } else {
            ContentUnavailableView(
                "No Record Selected",
                systemImage: "doc.text.magnifyingglass",
                description: Text("Pick a row to see its key, value and headers.")
            )
        }
    }

    @ViewBuilder
    private func metadata(
        for record: KafkaRecord,
        value: FormattedPayload,
        key: FormattedPayload
    ) -> some View {
        HStack(alignment: .top, spacing: 24) {
            InspectorField("Offset", "\(record.offset)")
            InspectorField("Partition", "\(record.partition)")
            InspectorField(
                "Timestamp",
                record.timestamp?.formatted(date: .numeric, time: .standard) ?? "—"
            )
            InspectorField("Timestamp Kind", kindLabel(record.timestampKind))
            InspectorField("Value Size", "\(record.valueByteCount) bytes")

            Spacer()

            // Only structured payloads have a pretty form to toggle to.
            if value.pretty != nil || key.pretty != nil {
                Picker("", selection: $showsPretty) {
                    Text("Pretty").tag(true)
                    Text("Raw").tag(false)
                }
                .pickerStyle(.segmented)
                .labelsHidden()
                .frame(width: 140)
            }

            if !topic.isEmpty {
                SaveRecordMenu(record: record, topic: topic)
            }
        }
    }

    /// The decoded Avro record, or why it could not be decoded.
    ///
    /// Shown above the raw value rather than replacing it: the bytes are still
    /// what is on the topic, and when decoding fails the hex is the only thing
    /// left to look at.
    @ViewBuilder
    private func avroBlock(for record: KafkaRecord) -> some View {
        switch avro {
        case .none:
            EmptyView()

        case .decoding:
            HStack(spacing: 6) {
                ProgressView().controlSize(.small)
                Text("Decoding Avro…").font(.caption).foregroundStyle(.secondary)
            }

        case .decoded(let decoded):
            VStack(alignment: .leading, spacing: 4) {
                HStack(spacing: 6) {
                    Text("Value (Avro)").font(.caption).foregroundStyle(.secondary)
                    Badge(text: "schema \(decoded.schemaID)", tint: .blue)
                    Spacer()
                    Picker("", selection: $showsSchema) {
                        Text("Record").tag(false)
                        Text("Schema").tag(true)
                    }
                    .pickerStyle(.segmented)
                    .labelsHidden()
                    .frame(width: 150)
                }

                Text(showsSchema ? prettySchema(decoded.schemaText) : decoded.json)
                    .monospaced()
                    .font(.callout)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(8)
                    .background(.quaternary, in: RoundedRectangle(cornerRadius: 6))
            }

        case .failed(let message):
            VStack(alignment: .leading, spacing: 4) {
                HStack(spacing: 6) {
                    Text("Value (Avro)").font(.caption).foregroundStyle(.secondary)
                    if let id = AvroPayload.schemaID(of: record.value) {
                        Badge(text: "schema \(id)", tint: .orange)
                    }
                    Badge(text: "Not decoded", tint: .orange)
                }
                Text(message)
                    .font(.callout)
                    .foregroundStyle(.orange)
                    .textSelection(.enabled)
            }
        }
    }

    /// Decodes the record's value if it is Confluent-framed.
    private func decodeAvro(_ record: KafkaRecord) async {
        guard AvroPayload.isFramed(record.value) else {
            avro = .none
            return
        }

        avro = .decoding
        do {
            if let decoded = try await connection?.decodeAvro(record.value) {
                avro = .decoded(decoded)
            } else {
                // No connection at all, which the browser cannot reach anyway.
                avro = .failed(SchemaRegistryError.notConfigured.localizedDescription)
            }
        } catch {
            avro = .failed((error as? LocalizedError)?.errorDescription ?? error.localizedDescription)
        }
    }

    /// Reformats the schema document, which the registry stores minified.
    private func prettySchema(_ text: String) -> String {
        PayloadFormatter.format(Data(text.utf8)).pretty ?? text
    }

    @ViewBuilder
    private func headers(of record: KafkaRecord) -> some View {
        if record.headers.isEmpty {
            Text("No headers")
                .font(.caption)
                .foregroundStyle(.secondary)
        } else {
            VStack(alignment: .leading, spacing: 4) {
                Text("Headers")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                ForEach(record.headers) { header in
                    HStack(alignment: .top) {
                        Text(header.name).monospaced().bold()
                        Text(header.displayValue).monospaced()
                    }
                    .font(.callout)
                    .textSelection(.enabled)
                }
            }
        }
    }

    private func kindLabel(_ kind: RecordTimestampKind) -> String {
        switch kind {
        case .createTime: "Create time"
        case .logAppendTime: "Log append time"
        case .unavailable: "Not available"
        }
    }
}

/// A payload with its format badge and, when parsing failed, the reason.
private struct PayloadBlock: View {
    let title: String
    let payload: FormattedPayload
    let showsPretty: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack(spacing: 6) {
                Text(title)
                    .font(.caption)
                    .foregroundStyle(.secondary)

                Badge(text: payload.kind.rawValue, tint: .secondary)

                if payload.isMalformed {
                    // The acceptance line for this slice: malformed structured
                    // text stays raw and says why.
                    Badge(text: "Malformed", tint: .orange)
                }
                if payload.kind == .binary {
                    Badge(text: "hex + ASCII", tint: .secondary)
                }
            }

            if let problem = payload.problem {
                Text(problem)
                    .font(.caption)
                    .foregroundStyle(.orange)
                    .textSelection(.enabled)
            }

            Text(payload.text(pretty: showsPretty))
                .monospaced()
                .font(.callout)
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(8)
                .background(.quaternary, in: RoundedRectangle(cornerRadius: 6))
        }
    }
}

/// A small capsule label.
struct Badge: View {
    let text: String
    let tint: Color

    var body: some View {
        Text(text)
            .font(.caption2)
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .background(tint.opacity(0.18), in: Capsule())
            .foregroundStyle(tint)
    }
}
