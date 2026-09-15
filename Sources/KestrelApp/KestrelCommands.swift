import SwiftUI

/// Menu bar layout: the standard Kestrel / File / Edit / View / Window / Help
/// menus from SwiftUI, plus the Cluster, Topic, and Tools menus.
///
/// Items whose feature has not shipped yet are present but disabled so the menu
/// structure is reviewable; later slices attach the actions.
struct KestrelCommands: Commands {
    let store: ClusterStore

    var body: some Commands {
        CommandGroup(replacing: .appInfo) {
            AboutMenuItem()
        }

        CommandGroup(replacing: .newItem) {
            Button("New Cluster…") { store.beginAdd() }
                .keyboardShortcut("n", modifiers: .command)
        }

        CommandGroup(after: .toolbar) {
            Toggle("Show Internal Topics", isOn: Binding(
                get: { store.showsInternalTopics },
                set: { store.showsInternalTopics = $0 }
            ))
        }

        CommandMenu("Cluster") {
            Button("Add Cluster…") { store.beginAdd() }
            Button("Edit Cluster…") {
                if let id = store.selection?.clusterID { store.beginEdit(id: id) }
            }
            .disabled(store.selection == nil)
            Divider()
            Button("Connect") {
                if let id = store.selection?.clusterID {
                    Task { await store.connect(id: id) }
                }
            }
            .disabled(store.selection == nil)
            Button("Disconnect") {
                if let id = store.selection?.clusterID { store.disconnect(id: id) }
            }
            .disabled(store.connection(for: store.selection?.clusterID) == nil)
            Button("Refresh") {
                if let id = store.selection?.clusterID {
                    Task { await store.refresh(id: id) }
                }
            }
            .keyboardShortcut("r", modifiers: .command)
            .disabled(store.selection == nil)
            Divider()
            Button("Reload Cluster List") { store.reload() }
        }

        CommandMenu("Topic") {
            Button("Create Topic…") {
                if let clusterID = store.selection?.clusterID {
                    store.topicSheet = .create(clusterID)
                }
            }
            .disabled(!store.isSelectionConnected)

            Button("Delete Topic…") {
                if case .topic(let clusterID, let name) = store.selection {
                    store.topicDeletion = TopicDeletion(clusterID: clusterID, topic: name)
                }
            }
            .disabled(!store.canProduceToSelection)
            Divider()
            Button("Produce Message…") {
                if case .topic(let clusterID, let name) = store.selection {
                    store.produceTarget = ProduceTarget(clusterID: clusterID, topic: name)
                }
            }
            .keyboardShortcut("p", modifiers: [.command, .shift])
            .disabled(!store.canProduceToSelection)
        }

        CommandMenu("Tools") {
            Button("Find Messages…") {
                if let clusterID = store.selection?.clusterID {
                    var topic: String?
                    if case .topic(_, let name) = store.selection { topic = name }
                    store.findTarget = FindTarget(clusterID: clusterID, topic: topic)
                }
            }
            .disabled(!store.isSelectionConnected)
                .keyboardShortcut("f", modifiers: [.command, .shift])
            Divider()
            Button("Import…") {
                if let clusterID = store.selection?.clusterID {
                    var topic: String?
                    if case .topic(_, let name) = store.selection { topic = name }
                    store.importTarget = ImportTarget(clusterID: clusterID, topic: topic)
                }
            }
            .disabled(!store.isSelectionConnected)
            Button("Export…") {
                if let clusterID = store.selection?.clusterID {
                    var topic: String?
                    if case .topic(_, let name) = store.selection { topic = name }
                    store.exportTarget = ExportTarget(clusterID: clusterID, topic: topic)
                }
            }
            .disabled(!store.isSelectionConnected)
            Button("Generate Test Data…") {
                if let clusterID = store.selection?.clusterID {
                    var topic: String?
                    if case .topic(_, let name) = store.selection { topic = name }
                    store.generateTarget = GenerateTarget(clusterID: clusterID, topic: topic)
                }
            }
            .disabled(!store.isSelectionConnected)
        }
    }
}

/// `Commands` bodies cannot read the environment, so the About item lives in a
/// small view that can.
private struct AboutMenuItem: View {
    @Environment(\.openWindow) private var openWindow

    var body: some View {
        Button("About Kestrel") { openWindow(id: WindowID.about) }
    }
}
