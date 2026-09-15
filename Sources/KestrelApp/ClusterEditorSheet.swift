import KestrelKit
import SwiftUI

/// Add / edit form for a cluster connection.
///
/// Password fields are bound to the draft, not to the profile, so they are only
/// ever written to the Keychain.
struct ClusterEditorSheet: View {
    @Environment(ClusterStore.self) private var store
    @State var draft: ClusterDraft
    let onSave: (ClusterDraft) -> Void
    let onCancel: () -> Void

    private var canSave: Bool {
        !draft.profile.bootstrapServers.trimmingCharacters(in: .whitespaces).isEmpty
    }

    var body: some View {
        VStack(spacing: 0) {
            Form {
                Section("General") {
                    TextField("Name", text: $draft.profile.name, prompt: Text("Local"))
                    TextField(
                        "Bootstrap Servers",
                        text: $draft.profile.bootstrapServers,
                        prompt: Text("host:9092, host2:9092")
                    )
                }

                Section("Security") {
                    Picker("Protocol", selection: $draft.profile.securityProtocol) {
                        ForEach(SecurityProtocol.allCases) { Text($0.rawValue).tag($0) }
                    }

                    if draft.profile.securityProtocol.usesSASL {
                        Picker("SASL Mechanism", selection: $draft.profile.saslMechanism) {
                            Text("Select…").tag(SASLMechanism?.none)
                            ForEach(SASLMechanism.allCases) { Text($0.rawValue).tag(SASLMechanism?.some($0)) }
                        }
                        TextField("Username", text: $draft.profile.saslUsername)
                        SecureField("Password", text: $draft.saslPassword)
                    }
                }

                if draft.profile.securityProtocol.usesTLS {
                    Section("TLS") {
                        FileField("CA Certificate", path: $draft.profile.tls.caLocation)
                        FileField("Client Certificate", path: $draft.profile.tls.certificateLocation)
                        FileField("Client Key", path: $draft.profile.tls.keyLocation)
                        SecureField("Key Passphrase", text: $draft.tlsKeyPassphrase)
                        Toggle("Verify hostname", isOn: $draft.profile.tls.verifyHostname)
                    }
                }

                Section("Schema Registry") {
                    TextField(
                        "URL",
                        text: $draft.profile.schemaRegistry.url,
                        prompt: Text("http://localhost:8081")
                    )
                    TextField("Username", text: $draft.profile.schemaRegistry.user)
                    SecureField("Password", text: $draft.schemaRegistryPassword)
                    Text("Needed to decode Avro records. Leave the username empty if the registry has no authentication.")
                        .font(.callout)
                        .foregroundStyle(.secondary)
                    if draft.profile.schemaRegistry.isConfigured {
                        HStack {
                            Button("Test Registry") {
                                Task { await store.testDraftRegistry(draft) }
                            }
                            .disabled(store.draftRegistryState.isTesting)
                            RegistryStatusText(state: store.draftRegistryState)
                        }
                    }
                }

                Section("Kafka Connect") {
                    TextField(
                        "URL",
                        text: $draft.profile.connect.url,
                        prompt: Text("http://localhost:8083")
                    )
                    TextField("Username", text: $draft.profile.connect.user)
                    SecureField("Password", text: $draft.connectPassword)
                    Text("Adds a Connect branch to this cluster, listing connectors and their tasks.")
                        .font(.callout)
                        .foregroundStyle(.secondary)
                    if draft.profile.connect.isConfigured {
                        HStack {
                            Button("Test Connect") {
                                Task { await store.testDraftConnect(draft) }
                            }
                            .disabled(store.draftConnectState.isTesting)
                            RegistryStatusText(state: store.draftConnectState)
                        }
                    }
                }

                Section("Status") {
                    ConnectionStatusRow(state: store.draftConnectionState)
                    Label(
                        "Passwords and passphrases are stored in the macOS Keychain, never in the cluster list.",
                        systemImage: "lock.fill"
                    )
                    .font(.callout)
                    .foregroundStyle(.secondary)
                }
            }
            .formStyle(.grouped)

            Divider()

            HStack {
                Button("Test Connection") {
                    Task { await store.testDraftConnection(draft) }
                }
                .disabled(!canSave || store.draftConnectionState.isTesting)
                Spacer()
                Button("Cancel", role: .cancel, action: onCancel)
                    .keyboardShortcut(.cancelAction)
                Button(draft.isNew ? "Add" : "Save") { onSave(draft) }
                    .keyboardShortcut(.defaultAction)
                    .buttonStyle(.borderedProminent)
                    .disabled(!canSave)
            }
            .padding(16)
        }
        .frame(width: 520, height: 620)
        .navigationTitle(draft.isNew ? "Add Cluster" : "Edit Cluster")
    }
}

/// Result of the registry test button.
private struct RegistryStatusText: View {
    let state: ClusterStore.RegistryState

    var body: some View {
        switch state {
        case .idle:
            EmptyView()
        case .testing:
            HStack(spacing: 6) {
                ProgressView().controlSize(.small)
                Text("Checking…").foregroundStyle(.secondary)
            }
        case .succeeded(let summary):
            Label(summary, systemImage: "checkmark.circle.fill")
                .foregroundStyle(.green)
                .font(.callout)
        case .failed(let message):
            Label(message, systemImage: "exclamationmark.triangle.fill")
                .foregroundStyle(.orange)
                .font(.callout)
                .textSelection(.enabled)
        }
    }
}

/// Text field for a file path with a "Choose…" button.
private struct FileField: View {
    let title: String
    @Binding var path: String

    init(_ title: String, path: Binding<String>) {
        self.title = title
        self._path = path
    }

    var body: some View {
        HStack {
            TextField(title, text: $path)
            Button("Choose…") {
                let panel = NSOpenPanel()
                panel.canChooseFiles = true
                panel.canChooseDirectories = false
                panel.allowsMultipleSelection = false
                if panel.runModal() == .OK, let url = panel.url {
                    path = url.path
                }
            }
        }
    }
}
