import AppKit
import KestrelKit
import SwiftUI

/// Saves a record to a file the user picks.
///
/// Uses `NSSavePanel` directly rather than SwiftUI's `fileExporter`, because the
/// suggested name and extension depend on the chosen format, and the panel is
/// opened from a menu rather than driven by a binding.
enum RecordSaver {
    /// What came of a save attempt.
    enum Outcome {
        case saved(String)
        case failed(String)
        case cancelled
    }

    /// Asks for a location, then writes the record.
    ///
    /// - Parameters:
    ///   - record: record to write.
    ///   - topic: topic it came from, recorded in the envelope.
    ///   - format: layout to write.
    /// - Returns: the outcome, including the message to show. Errors come back
    ///   as text rather than thrown, since the caller only displays them.
    @MainActor
    static func save(
        record: KafkaRecord,
        topic: String,
        format: RecordSaveFormat
    ) -> Outcome {
        let panel = NSSavePanel()
        panel.title = "Save Record"
        panel.message = format.label
        panel.nameFieldStringValue = suggestedName(record: record, topic: topic, format: format)
        panel.canCreateDirectories = true

        guard panel.runModal() == .OK, let url = panel.url else { return .cancelled }

        do {
            let byteCount = try RecordFile.write(
                record: record,
                topic: topic,
                format: format,
                to: url
            )
            return .saved("Saved \(byteCount) bytes to \(url.lastPathComponent)")
        } catch {
            return .failed("Could not save: \(error.localizedDescription)")
        }
    }

    /// Builds a filename from the topic, partition and offset.
    ///
    /// Delegates to ``RecordSaveFormat/suggestedNameForChecks(record:topic:format:)``
    /// so the naming rule has one home and stays verifiable.
    static func suggestedName(
        record: KafkaRecord,
        topic: String,
        format: RecordSaveFormat
    ) -> String {
        RecordSaveFormat.suggestedNameForChecks(record: record, topic: topic, format: format)
    }
}

/// The Save menu shown in the record detail pane.
struct SaveRecordMenu: View {
    let record: KafkaRecord
    let topic: String

    @Environment(ClusterStore.self) private var store

    var body: some View {
        Menu {
            ForEach(RecordSaveFormat.allCases, id: \.rawValue) { format in
                Button(format.label) { save(as: format) }
            }
        } label: {
            Label("Save…", systemImage: "square.and.arrow.down")
        }
        .menuStyle(.borderlessButton)
        .fixedSize()
    }

    private func save(as format: RecordSaveFormat) {
        switch RecordSaver.save(record: record, topic: topic, format: format) {
        case .saved(let message):
            store.toast = message
        case .failed(let message):
            store.errorMessage = message
        case .cancelled:
            break
        }
    }
}
