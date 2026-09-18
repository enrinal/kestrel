import Foundation
import KestrelKit

/// An in-flight edit of a cluster profile.
///
/// Secrets live here only while the sheet is open; on save they go to the
/// Keychain and never touch the profile that is written to disk.
struct ClusterDraft: Identifiable {
    var profile: ClusterProfile
    var saslPassword: String = ""
    var tlsKeyPassphrase: String = ""
    var schemaRegistryPassword: String = ""
    var connectPassword: String = ""
    let isNew: Bool

    var id: UUID { profile.id }

    static func new() -> ClusterDraft {
        ClusterDraft(
            profile: ClusterProfile(name: "", bootstrapServers: "localhost:9092"),
            isNew: true
        )
    }
}

/// The app's cluster list, backed by `clusters.json` plus Keychain secrets, and
/// the live connections opened from it.
@MainActor
@Observable
final class ClusterStore {
    /// Progress of a connection test started from the editor sheet.
    enum ConnectionState {
        case idle
        case testing
        case tested(ConnectionTestResult)

        var isTesting: Bool {
            if case .testing = self { return true }
            return false
        }
    }

    /// Progress of a Schema Registry test started from the editor sheet.
    enum RegistryState {
        case idle
        case testing
        case succeeded(String)
        case failed(String)

        var isTesting: Bool {
            if case .testing = self { return true }
            return false
        }
    }

    private(set) var clusters: [ClusterProfile] = []
    var selection: SidebarItem?
    var draft: ClusterDraft?
    var errorMessage: String?
    /// Which pane the topic inspector shows.
    ///
    /// Held here so it survives switching between topics, rather than snapping
    /// back to Partitions each time.
    var topicPane: TopicPane = .partitions
    /// Selected record in the message browser.
    ///
    /// Kept here rather than in the browser's own state so snapshots can drive
    /// a selection, the same reason `expandedItems` lives here.
    var selectedRecord: KafkaRecord.ID?
    /// Which topic-management sheet is open, if any.
    var topicSheet: TopicSheet?
    /// Topic awaiting delete confirmation.
    var topicDeletion: TopicDeletion?
    /// Cluster the find sheet is scanning, when it is open.
    var findTarget: FindTarget?
    /// Offset the message browser should open next, set by a search hit.
    ///
    /// The browser clears it once it has honoured it, so returning to a topic
    /// later does not jump again.
    var recordJump: RecordJump?
    /// Cluster the generate sheet is writing to, when it is open.
    var generateTarget: GenerateTarget?
    /// Cluster the export sheet is reading from, when it is open.
    var exportTarget: ExportTarget?
    /// Cluster the import sheet is writing to, when it is open.
    var importTarget: ImportTarget?
    /// Topic the produce sheet is writing to, when it is open.
    var produceTarget: ProduceTarget?
    /// Transient confirmation banner; cleared on a timer by `RootView`.
    var toast: String?

    /// Connection test state for the editor sheet, which tests unsaved values.
    private(set) var draftConnectionState: ConnectionState = .idle

    /// Whether the sidebar lists Kafka's internal `__` topics.
    var showsInternalTopics = false

    /// Narrows the sidebar's topic and consumer group lists to matching names.
    var sidebarFilter = ""

    /// How many rows one sidebar branch draws before it stops.
    ///
    /// `List` only *displays* the rows on screen, but it still builds a view
    /// for every element of the `ForEach` on each pass, and it makes a pass on
    /// every change to this store — every selection, expand and hover. Counted
    /// against a 2,021-topic broker: uncapped, reaching a settled window built
    /// 4,000+ topic rows; capped, 500. That per-change cost is the scroll and
    /// click lag, so the cap is on how many rows are offered, not drawn.
    ///
    /// A list that long cannot be read by eye anyway. `sidebarFilter` is how
    /// you reach what the cut-off hides, and the branch row says so.
    static let sidebarRowLimit = 300

    /// True when the selection belongs to a connected cluster.
    var isSelectionConnected: Bool {
        guard let clusterID = selection?.clusterID else { return false }
        return connection(for: clusterID)?.phase == .connected
    }

    /// True when a connected cluster's topic is selected, so a record can be sent.
    var canProduceToSelection: Bool {
        guard case .topic(let clusterID, _) = selection else { return false }
        return connection(for: clusterID)?.phase == .connected
    }

    /// Which sidebar branches are open. Held here rather than in view state so
    /// the tree can be expanded programmatically and, later, restored.
    private var expandedItems: Set<SidebarItem> = []

    private var connections: [UUID: ClusterConnection] = [:]

    private let repository: ClusterProfileRepository
    private let keychain: KeychainStore

    init(
        repository: ClusterProfileRepository = ClusterProfileRepository(),
        keychain: KeychainStore = KeychainStore()
    ) {
        self.repository = repository
        self.keychain = keychain
        reload()
    }

    var storeLocation: String { repository.fileURL.path }

    func cluster(id: UUID?) -> ClusterProfile? {
        guard let id else { return nil }
        return clusters.first { $0.id == id }
    }

    /// The cluster the selection belongs to, whatever kind of node it is.
    var selectedCluster: ClusterProfile? {
        cluster(id: selection?.clusterID)
    }

    /// Loads the saved list from disk, replacing anything in memory.
    ///
    /// Open connections are left alone; reloading the list is not disconnecting.
    func reload() {
        do {
            clusters = try repository.load()
        } catch {
            clusters = []
            errorMessage = "Could not read \(repository.fileURL.lastPathComponent): \(error.localizedDescription)"
        }
    }

    // MARK: Editing

    func beginAdd() {
        draftConnectionState = .idle
        draft = .new()
    }

    /// Opens the editor for an existing profile, loading its secrets so the
    /// password fields show that something is stored.
    func beginEdit(id: UUID) {
        guard let profile = cluster(id: id) else { return }
        var draft = ClusterDraft(profile: profile, isNew: false)
        let secrets = secrets(for: profile)
        draft.saslPassword = secrets.saslPassword ?? ""
        draft.tlsKeyPassphrase = secrets.tlsKeyPassphrase ?? ""
        draftConnectionState = .idle
        self.draft = draft
    }

    /// Persists the draft: profile to JSON, secrets to the Keychain.
    ///
    /// Editing a connected cluster drops its connection, since the settings it
    /// was opened with may no longer apply.
    func commit(_ draft: ClusterDraft) {
        var profile = draft.profile
        profile.name = profile.name.trimmingCharacters(in: .whitespacesAndNewlines)
        if profile.name.isEmpty { profile.name = "Untitled Cluster" }
        if !profile.securityProtocol.usesSASL { profile.saslMechanism = nil }

        if let index = clusters.firstIndex(where: { $0.id == profile.id }) {
            clusters[index] = profile
        } else {
            clusters.append(profile)
        }

        do {
            try repository.save(clusters)
            try keychain.set(draft.saslPassword, secret: .saslPassword, cluster: profile.id)
            try keychain.set(draft.tlsKeyPassphrase, secret: .tlsKeyPassphrase, cluster: profile.id)
            try keychain.set(
                draft.schemaRegistryPassword,
                secret: .schemaRegistryPassword,
                cluster: profile.id
            )
            try keychain.set(draft.connectPassword, secret: .connectPassword, cluster: profile.id)
        } catch {
            errorMessage = "Could not save the cluster: \(error.localizedDescription)"
        }

        disconnect(id: profile.id)
        selection = .cluster(profile.id)
        self.draft = nil
    }

    func remove(id: UUID) {
        disconnect(id: id)
        clusters.removeAll { $0.id == id }
        if selection?.clusterID == id { selection = nil }
        do {
            try repository.save(clusters)
            try keychain.removeAll(cluster: id)
        } catch {
            errorMessage = "Could not remove the cluster: \(error.localizedDescription)"
        }
    }

    /// Fetches the secrets a profile's security protocol actually needs.
    ///
    /// Protocols that use neither SASL nor TLS skip the Keychain entirely, which
    /// avoids a pointless authorisation prompt for a PLAINTEXT cluster.
    private func secrets(for profile: ClusterProfile) -> ClusterSecrets {
        var secrets = ClusterSecrets()
        do {
            if profile.securityProtocol.usesSASL {
                secrets.saslPassword = try keychain.get(secret: .saslPassword, cluster: profile.id)
            }
            if profile.securityProtocol.usesTLS {
                secrets.tlsKeyPassphrase = try keychain.get(secret: .tlsKeyPassphrase, cluster: profile.id)
            }
            // Independent of the broker's protocol: a PLAINTEXT cluster can
            // still have a registry behind basic auth.
            if !profile.schemaRegistry.user.isEmpty {
                secrets.schemaRegistryPassword = try keychain.get(
                    secret: .schemaRegistryPassword,
                    cluster: profile.id
                )
            }
            if !profile.connect.user.isEmpty {
                secrets.connectPassword = try keychain.get(
                    secret: .connectPassword,
                    cluster: profile.id
                )
            }
        } catch {
            errorMessage = "Could not read secrets from the Keychain: \(error.localizedDescription)"
        }
        return secrets
    }

    // MARK: Tree expansion

    func isExpanded(_ item: SidebarItem) -> Bool {
        expandedItems.contains(item)
    }

    func setExpanded(_ item: SidebarItem, _ expanded: Bool) {
        if expanded {
            expandedItems.insert(item)
        } else {
            expandedItems.remove(item)
        }
    }

    /// Opens a cluster and all three of its branches.
    func expandAll(id: UUID) {
        expandedItems.formUnion([
            .cluster(id), .brokersFolder(id), .topicsFolder(id), .groupsFolder(id)
        ])
    }

    /// Opens a cluster and only the given branches, collapsing the others.
    func expand(id: UUID, branches: Set<SidebarItem>) {
        expandedItems.subtract([.brokersFolder(id), .topicsFolder(id), .groupsFolder(id)])
        expandedItems.insert(.cluster(id))
        expandedItems.formUnion(branches)
    }

    // MARK: Connections

    func connection(for id: UUID?) -> ClusterConnection? {
        guard let id else { return nil }
        return connections[id]
    }

    /// Opens a connection if needed and loads brokers and topics.
    func connect(id: UUID) async {
        guard let profile = cluster(id: id) else { return }

        let connection: ClusterConnection
        if let existing = connections[id] {
            connection = existing
        } else {
            do {
                connection = try ClusterConnection(profile: profile, secrets: secrets(for: profile))
                connections[id] = connection
            } catch {
                errorMessage = (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
                return
            }
        }

        await connection.connect()
    }

    /// Drops the connection and everything loaded through it.
    func disconnect(id: UUID) {
        connections[id] = nil
    }

    func refresh(id: UUID) async {
        guard let connection = connections[id] else {
            await connect(id: id)
            return
        }
        await connection.refresh()
    }

    /// Topics for the sidebar, honouring the internal-topics preference and
    /// the sidebar filter.
    func visibleTopics(for id: UUID) -> [TopicInfo] {
        guard let connection = connections[id] else { return [] }
        let topics = showsInternalTopics
            ? connection.topics
            : connection.topics.filter { !$0.isInternal }
        return Self.matching(topics, filter: sidebarFilter, name: \.name)
    }

    /// Consumer groups for the sidebar, honouring the sidebar filter.
    ///
    /// Filtered by the same field as topics: a cluster with hundreds of groups
    /// hits `sidebarRowLimit` too, and a cut-off list you cannot narrow would
    /// be a list with rows you can never reach.
    func visibleGroups(for id: UUID) -> [ConsumerGroupInfo] {
        guard let connection = connections[id] else { return [] }
        return Self.matching(connection.groups, filter: sidebarFilter, name: \.id)
    }

    /// Keeps the items whose name contains `filter`, case- and diacritic-insensitively.
    private static func matching<Item>(
        _ items: [Item],
        filter: String,
        name: (Item) -> String
    ) -> [Item] {
        let wanted = filter.trimmingCharacters(in: .whitespaces)
        guard !wanted.isEmpty else { return items }
        return items.filter { name($0).localizedCaseInsensitiveContains(wanted) }
    }

    /// Result of the last registry test in the editor sheet.
    var draftRegistryState: RegistryState = .idle
    /// Result of the last Connect test in the editor sheet.
    var draftConnectState: RegistryState = .idle

    /// Tests the Kafka Connect values currently in the editor sheet.
    ///
    /// Its own button and its own state, for the same reason the registry has
    /// one: broker, registry and Connect fail independently, and a single
    /// verdict would hide which of the three is broken.
    func testDraftConnect(_ draft: ClusterDraft) async {
        draftConnectState = .testing
        do {
            let client = try KafkaConnectClient(
                url: draft.profile.connect.url,
                user: draft.profile.connect.user,
                password: draft.connectPassword
            )
            draftConnectState = .succeeded(try await client.test())
        } catch {
            draftConnectState = .failed(
                (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
            )
        }
    }

    /// Tests the Schema Registry values currently in the editor sheet.
    ///
    /// Separate from the broker test because the two fail independently: a
    /// reachable cluster with an unreachable registry is a common state, and
    /// one button reporting both would hide which half is broken.
    func testDraftRegistry(_ draft: ClusterDraft) async {
        draftRegistryState = .testing
        do {
            let client = try SchemaRegistryClient(
                url: draft.profile.schemaRegistry.url,
                user: draft.profile.schemaRegistry.user,
                password: draft.schemaRegistryPassword
            )
            draftRegistryState = .succeeded(try await client.test())
        } catch {
            draftRegistryState = .failed(
                (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
            )
        }
    }

    // MARK: Connection test (editor sheet)

    /// Tests the values currently in the editor sheet, saved or not.
    func testDraftConnection(_ draft: ClusterDraft) async {
        draftConnectionState = .testing
        let secrets = ClusterSecrets(
            saslPassword: draft.saslPassword,
            tlsKeyPassphrase: draft.tlsKeyPassphrase
        )

        let result: ConnectionTestResult
        do {
            let client = try KafkaClient(profile: draft.profile, secrets: secrets)
            result = await client.testConnection(timeout: .seconds(10))
        } catch {
            result = .failure((error as? LocalizedError)?.errorDescription ?? error.localizedDescription)
        }
        draftConnectionState = .tested(result)
    }
}
