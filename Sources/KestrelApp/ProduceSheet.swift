import KestrelKit
import SwiftUI

/// Identifies the topic a produce sheet is writing to.
struct ProduceTarget: Identifiable, Equatable {
    let clusterID: UUID
    let topic: String

    var id: String { "\(clusterID):\(topic)" }
}

/// One editable header row.
private struct HeaderDraft: Identifiable, Equatable {
    let id = UUID()
    var name = ""
    var value = ""
}

/// Composes and sends a single record.
struct ProduceSheet: View {
    let target: ProduceTarget

    @Environment(ClusterStore.self) private var store
    @Environment(\.dismiss) private var dismiss

    @State private var partitionChoice = PartitionChoice.automatic
    @State private var partition: Int32 = 0
    @State private var key = ""
    @State private var value = ""
    @State private var isTombstone = false
    @State private var headers: [HeaderDraft] = []
    @State private var isSending = false
    @State private var failure: String?
    @State private var valueFormat = ValueFormat.text
    @State private var subject = ""
    @State private var subjects: [String] = []
    @State private var subjectsProblem: String?
    @State private var isLoadingSubjects = false

    /// How to turn the value editor's text into bytes.
    private enum ValueFormat: String, CaseIterable, Identifiable {
        case text = "Text"
        case avro = "Avro"

        var id: String { rawValue }
    }

    /// Whether to let the partitioner choose.
    private enum PartitionChoice: String, CaseIterable, Identifiable {
        case automatic = "Automatic"
        case specific = "Specific"

        var id: String { rawValue }
    }

    private var connection: ClusterConnection? { store.connection(for: target.clusterID) }
    private var partitions: [PartitionInfo] {
        connection?.topic(named: target.topic)?.partitions ?? []
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
            Divider()
            form
            Divider()
            footer
        }
        .frame(width: 560, height: 620)
        .task {
            if Snapshot.producesAvro, connection?.registry != nil { valueFormat = .avro }
            await loadSubjects()
        }
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text("Produce Message")
                .font(.headline)
            Text(target.topic)
                .font(.caption)
                .foregroundStyle(.secondary)
                .textSelection(.enabled)
        }
        .padding(12)
    }

    private var form: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 14) {
                HStack {
                    Picker("Partition", selection: $partitionChoice) {
                        ForEach(PartitionChoice.allCases) { Text($0.rawValue).tag($0) }
                    }
                    .frame(width: 200)

                    if partitionChoice == .specific {
                        Picker("", selection: $partition) {
                            ForEach(partitions) { Text("\($0.id)").tag($0.id) }
                        }
                        .labelsHidden()
                        .frame(width: 90)
                    } else {
                        Text(key.isEmpty ? "Round robin" : "By key hash")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    Spacer()
                }

                LabelledEditor(title: "Key", text: $key, height: 56)

                valueFormatRow

                VStack(alignment: .leading, spacing: 4) {
                    Toggle("Send as tombstone (null value)", isOn: $isTombstone)
                        .font(.callout)
                    // A tombstone is a null value, which is not the same as an
                    // empty one, so the editor is disabled rather than cleared.
                    LabelledEditor(
                        title: "Value",
                        text: $value,
                        height: 150,
                        isDisabled: isTombstone
                    )
                }

                headerEditor

                if let failure {
                    Text(failure)
                        .font(.callout)
                        .foregroundStyle(.red)
                        .textSelection(.enabled)
                }
            }
            .padding(12)
        }
    }

    /// Text or Avro, and which subject's schema to encode against.
    ///
    /// The Avro option is only offered when the cluster has a registry: without
    /// one there is no schema to encode against and no id to frame with.
    @ViewBuilder
    private var valueFormatRow: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Picker("Value Format", selection: $valueFormat) {
                    ForEach(ValueFormat.allCases) { Text($0.rawValue).tag($0) }
                }
                .frame(width: 220)
                .disabled(connection?.registry == nil)

                if connection?.registry == nil {
                    Text("Avro needs a Schema Registry for this cluster.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
            }

            if valueFormat == .avro {
                HStack {
                    Picker("Subject", selection: $subject) {
                        if subject.isEmpty || !subjects.contains(subject) {
                            Text("Select…").tag("")
                        }
                        ForEach(subjects, id: \.self) { Text($0).tag($0) }
                    }
                    if isLoadingSubjects {
                        ProgressView().controlSize(.small)
                    }
                    Spacer()
                }

                if let subjectsProblem {
                    Text(subjectsProblem)
                        .font(.caption)
                        .foregroundStyle(.orange)
                        .textSelection(.enabled)
                } else {
                    Text("The value is read as JSON and encoded against the subject's newest schema.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
        }
    }

    private var headerEditor: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text("Headers")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
                Button {
                    headers.append(HeaderDraft())
                } label: {
                    Label("Add", systemImage: "plus")
                }
                .buttonStyle(.borderless)
            }

            if headers.isEmpty {
                Text("No headers")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                ForEach($headers) { $header in
                    HStack {
                        TextField("Name", text: $header.name)
                        TextField("Value", text: $header.value)
                        Button {
                            headers.removeAll { $0.id == header.id }
                        } label: {
                            Image(systemName: "minus.circle")
                        }
                        .buttonStyle(.borderless)
                    }
                }
            }
        }
    }

    private var footer: some View {
        HStack {
            if isSending {
                ProgressView().controlSize(.small)
            }
            Spacer()
            Button("Cancel") { dismiss() }
                .keyboardShortcut(.cancelAction)
            Button("Send") { send() }
                .keyboardShortcut(.defaultAction)
                .buttonStyle(.borderedProminent)
                .disabled(isSending || !isValid)
        }
        .padding(12)
    }

    /// Named headers only; an unnamed header cannot be sent. Avro also needs a
    /// subject, since it decides both the encoding and the schema id.
    private var isValid: Bool {
        guard headers.allSatisfy({ !$0.name.isEmpty }) else { return false }
        if valueFormat == .avro, !isTombstone, subject.isEmpty { return false }
        return true
    }

    /// Loads the registry's subjects, and preselects the topic's own.
    ///
    /// `<topic>-value` is Confluent's default naming, so it is nearly always
    /// the right subject for a topic and is worth selecting unasked.
    private func loadSubjects() async {
        guard let connection, connection.registry != nil else { return }
        isLoadingSubjects = true
        do {
            subjects = try await connection.subjects()
            let preferred = "\(target.topic)-value"
            if subjects.contains(preferred) { subject = preferred }
            if subjects.isEmpty {
                subjectsProblem = "The registry has no subjects yet, so there is no schema to encode against."
            }
        } catch {
            subjectsProblem = (error as? LocalizedError)?.errorDescription
                ?? error.localizedDescription
        }
        isLoadingSubjects = false
    }

    private func send() {
        guard let connection else { return }
        isSending = true
        failure = nil

        Task {
            do {
                let body: Data?
                if isTombstone {
                    body = nil
                } else if valueFormat == .avro {
                    body = try await connection.encodeAvro(json: value, subject: subject)
                } else {
                    body = Data(value.utf8)
                }

                let report = try await connection.produce(
                    topic: target.topic,
                    partition: partitionChoice == .specific ? partition : nil,
                    key: key.isEmpty ? nil : Data(key.utf8),
                    value: body,
                    headers: headers.map {
                        RecordHeader(name: $0.name, value: Data($0.value.utf8))
                    }
                )
                store.toast = """
                    Produced to \(target.topic) partition \(report.partition), \
                    offset \(report.offset)
                    """
                dismiss()
            } catch {
                failure = ClusterConnection.message(for: error)
            }
            isSending = false
        }
    }
}

/// A titled multi-line text editor.
private struct LabelledEditor: View {
    let title: String
    @Binding var text: String
    let height: CGFloat
    var isDisabled = false

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            TextEditor(text: $text)
                .font(.callout)
                .monospaced()
                .frame(height: height)
                .disabled(isDisabled)
                .opacity(isDisabled ? 0.4 : 1)
                .overlay {
                    RoundedRectangle(cornerRadius: 6)
                        .stroke(.quaternary)
                }
        }
    }
}

/// A transient confirmation banner.
struct ToastView: View {
    let text: String

    var body: some View {
        Text(text)
            .font(.callout)
            .padding(.horizontal, 14)
            .padding(.vertical, 10)
            .background(.regularMaterial, in: Capsule())
            .overlay(Capsule().stroke(.quaternary))
            .shadow(radius: 8, y: 2)
            .padding(.bottom, 24)
            .transition(.move(edge: .bottom).combined(with: .opacity))
    }
}
