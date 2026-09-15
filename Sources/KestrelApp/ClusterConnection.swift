import Foundation
import KestrelKit

/// What the sidebar can select. Every case carries the cluster it belongs to so
/// the detail pane can find the right connection.
enum SidebarItem: Hashable {
    case cluster(UUID)
    case brokersFolder(UUID)
    case topicsFolder(UUID)
    case groupsFolder(UUID)
    case broker(UUID, Int32)
    case topic(UUID, String)
    case group(UUID, String)
    case connectFolder(UUID)
    case connector(UUID, String)

    var clusterID: UUID {
        switch self {
        case .cluster(let id),
             .brokersFolder(let id),
             .topicsFolder(let id),
             .groupsFolder(let id),
             .broker(let id, _),
             .topic(let id, _),
             .group(let id, _),
             .connectFolder(let id),
             .connector(let id, _):
            return id
        }
    }
}

/// A live connection to one cluster, plus the metadata the sidebar tree shows.
///
/// Created on connect and dropped on disconnect, so holding no connection means
/// holding no broker sockets.
@MainActor
@Observable
final class ClusterConnection {
    enum Phase: Equatable {
        case idle
        case connecting
        case connected
        case failed(String)
    }

    /// Load state for children fetched separately from cluster metadata.
    enum Load: Equatable {
        case notLoaded
        case loading
        case loaded
        case failed(String)
    }

    /// Identifies one partition of one topic.
    struct PartitionKey: Hashable {
        let topic: String
        let partition: Int32
    }

    let profile: ClusterProfile
    private let client: KafkaClient
    private let secrets: ClusterSecrets
    /// Created on the first record read; browsing a topic is opt-in.
    private var consumer: KafkaConsumer?
    /// Schema Registry client, when the cluster has one configured. Built once
    /// so its schema cache survives between records.
    let registry: SchemaRegistryClient?
    /// Why the registry client could not be built, if the URL is unusable.
    private(set) var registryProblem: String?
    /// Kafka Connect client, when the cluster has a worker configured.
    private let connectClient: KafkaConnectClient?
    /// Why the Connect client could not be built, if the URL is unusable.
    private(set) var connectProblem: String?

    private(set) var connectors: [ConnectorInfo] = []
    private(set) var connectorsLoad: Load = .notLoaded
    /// Connectors currently being paused, resumed or restarted, so the buttons
    /// can be disabled for just those rather than the whole pane.
    private(set) var busyConnectors: Set<String> = []

    private(set) var phase: Phase = .idle
    private(set) var brokers: [BrokerInfo] = []
    private(set) var topics: [TopicInfo] = []
    private(set) var groups: [ConsumerGroupInfo] = []
    private(set) var groupsLoad: Load = .notLoaded
    private(set) var lastRefreshed: Date?

    private var configEntries: [ConfigResource: [ConfigEntry]] = [:]
    private var configLoads: [ConfigResource: Load] = [:]
    private var recordPages: [PartitionKey: RecordPage] = [:]
    private var recordLoads: [PartitionKey: Load] = [:]
    private var groupOffsetRows: [String: [GroupPartitionOffset]] = [:]
    private var groupOffsetLoads: [String: Load] = [:]

    init(profile: ClusterProfile, secrets: ClusterSecrets) throws {
        self.profile = profile
        self.secrets = secrets
        self.client = try KafkaClient(profile: profile, secrets: secrets)

        // A bad registry URL must not stop the cluster connecting: everything
        // except Avro decoding still works without it.
        if profile.schemaRegistry.isConfigured {
            do {
                self.registry = try SchemaRegistryClient(
                    url: profile.schemaRegistry.url,
                    user: profile.schemaRegistry.user,
                    password: secrets.schemaRegistryPassword
                )
            } catch {
                self.registry = nil
                self.registryProblem = (error as? LocalizedError)?.errorDescription
                    ?? error.localizedDescription
            }
        } else {
            self.registry = nil
        }

        if profile.connect.isConfigured {
            do {
                self.connectClient = try KafkaConnectClient(
                    url: profile.connect.url,
                    user: profile.connect.user,
                    password: secrets.connectPassword
                )
            } catch {
                self.connectClient = nil
                self.connectProblem = (error as? LocalizedError)?.errorDescription
                    ?? error.localizedDescription
            }
        } else {
            self.connectClient = nil
        }
    }

    // MARK: Kafka Connect

    /// True when the cluster has a Connect URL, whether or not it works.
    ///
    /// The sidebar branch appears on this, not on a successful request: hiding
    /// the branch when the worker is down would look like the setting had been
    /// lost.
    var hasConnect: Bool { profile.connect.isConfigured }

    /// Fetches the connector list.
    ///
    /// - Parameter force: refetch even if it has already loaded, which is what
    ///   the refresh button and the actions want.
    func loadConnectors(force: Bool = false) async {
        guard hasConnect else { return }
        guard force || connectorsLoad == .notLoaded else { return }

        if let connectProblem {
            connectorsLoad = .failed(connectProblem)
            return
        }
        guard let connectClient else {
            connectorsLoad = .failed(
                KafkaConnectError.notConfigured.localizedDescription
            )
            return
        }

        connectorsLoad = .loading
        do {
            connectors = try await connectClient.connectors()
            connectorsLoad = .loaded
        } catch {
            // The list is left as it was: a rebalance that fails one refresh
            // should not blank a pane that was showing good data.
            connectorsLoad = .failed(Self.message(for: error))
        }
    }

    func connector(named name: String) -> ConnectorInfo? {
        connectors.first { $0.name == name }
    }

    /// What a connector action is doing, for the button that started it.
    enum ConnectorAction: String {
        case pause = "Pause"
        case resume = "Resume"
        case restart = "Restart"
    }

    /// Runs a connector action, then refreshes the list.
    ///
    /// Connect answers pause and resume with 202 and changes state only once
    /// the worker has rebalanced, so the immediate refetch usually still shows
    /// the old state; the short wait covers the common case, and the Refresh
    /// button covers the rest.
    ///
    /// - Returns: an error message, or `nil` when the action was accepted.
    @discardableResult
    func run(_ action: ConnectorAction, on name: String) async -> String? {
        guard let connectClient else { return KafkaConnectError.notConfigured.localizedDescription }

        busyConnectors.insert(name)
        defer { busyConnectors.remove(name) }

        do {
            switch action {
            case .pause: try await connectClient.pause(name)
            case .resume: try await connectClient.resume(name)
            case .restart: try await connectClient.restart(name, includeTasks: true)
            }
            try? await Task.sleep(for: .milliseconds(800))
            await loadConnectors(force: true)
            return nil
        } catch {
            return Self.message(for: error)
        }
    }

    /// Restarts one task of a connector, then refreshes the list.
    @discardableResult
    func restartTask(_ task: Int, of name: String) async -> String? {
        guard let connectClient else { return KafkaConnectError.notConfigured.localizedDescription }

        busyConnectors.insert(name)
        defer { busyConnectors.remove(name) }

        do {
            try await connectClient.restartTask(task, of: name)
            try? await Task.sleep(for: .milliseconds(800))
            await loadConnectors(force: true)
            return nil
        } catch {
            return Self.message(for: error)
        }
    }

    var isConnected: Bool { phase == .connected }

    /// Fetches brokers and topics, which arrive together in one metadata call.
    func connect() async {
        guard phase != .connecting else { return }
        phase = .connecting
        do {
            let metadata = try await client.metadata()
            brokers = metadata.brokers.sorted { $0.id < $1.id }
            topics = metadata.topics.sorted { $0.name < $1.name }
            lastRefreshed = .now
            phase = .connected
        } catch {
            brokers = []
            topics = []
            phase = .failed(Self.message(for: error))
        }
    }

    /// Re-fetches metadata, and consumer groups and connectors too if they were
    /// ever shown.
    func refresh() async {
        await connect()
        if groupsLoad != .notLoaded {
            await loadGroups(force: true)
        }
        if connectorsLoad != .notLoaded {
            await loadConnectors(force: true)
        }
    }

    /// Fetches consumer groups.
    ///
    /// Called when the Consumer Groups branch is first expanded, so a cluster
    /// with many groups costs nothing until someone looks.
    ///
    /// - Parameter force: refetch even if the groups are already loaded.
    func loadGroups(force: Bool = false) async {
        guard phase == .connected else { return }
        if groupsLoad == .loading { return }
        if groupsLoad == .loaded, !force { return }

        groupsLoad = .loading
        do {
            groups = try await client.consumerGroups()
            groupsLoad = .loaded
        } catch {
            groups = []
            groupsLoad = .failed(Self.message(for: error))
        }
    }

    // MARK: Configuration

    func configs(for resource: ConfigResource) -> [ConfigEntry] {
        configEntries[resource] ?? []
    }

    func configLoad(for resource: ConfigResource) -> Load {
        configLoads[resource] ?? .notLoaded
    }

    /// Fetches a broker's or topic's configuration, once per resource.
    ///
    /// Called when an inspector opens, so browsing the tree never pays for
    /// configs nobody looked at.
    ///
    /// - Parameter force: refetch even if the configuration is already loaded.
    func loadConfigs(for resource: ConfigResource, force: Bool = false) async {
        guard phase == .connected else { return }
        let load = configLoad(for: resource)
        if load == .loading { return }
        if load == .loaded, !force { return }

        configLoads[resource] = .loading
        do {
            configEntries[resource] = try await client.describeConfigs(resource)
            configLoads[resource] = .loaded
        } catch {
            configEntries[resource] = []
            configLoads[resource] = .failed(Self.message(for: error))
        }
    }

    /// The broker's `broker.rack`, if its configuration has been read and the
    /// broker actually declares one.
    func rack(forBroker id: Int32) -> String? {
        configs(for: .broker(id))
            .first { $0.name == "broker.rack" }
            .flatMap { entry in
                guard let value = entry.value, !value.isEmpty else { return nil }
                return value
            }
    }

    // MARK: Records

    func page(for key: PartitionKey) -> RecordPage? {
        recordPages[key]
    }

    func recordLoad(for key: PartitionKey) -> Load {
        recordLoads[key] ?? .notLoaded
    }

    /// Reads a page of records from one partition and caches it.
    ///
    /// Reuses a single consumer for the cluster. The consumer assigns
    /// partitions directly and never commits, so browsing cannot disturb the
    /// consumer groups that own the topic.
    ///
    /// - Parameters:
    ///   - topic: topic name.
    ///   - partition: partition id.
    ///   - start: where the read begins.
    ///   - limit: maximum records to read.
    func loadRecords(
        topic: String,
        partition: Int32,
        from start: StartPosition,
        limit: Int
    ) async {
        guard phase == .connected else { return }
        let key = PartitionKey(topic: topic, partition: partition)
        guard recordLoad(for: key) != .loading else { return }

        recordLoads[key] = .loading
        do {
            let consumer = try activeConsumer()
            recordPages[key] = try await consumer.fetch(
                topic: topic,
                partition: partition,
                from: start,
                limit: limit
            )
            recordLoads[key] = .loaded
        } catch {
            recordPages[key] = nil
            recordLoads[key] = .failed(Self.message(for: error))
        }
    }

    /// Sends one record to this cluster and returns where it landed.
    ///
    /// - Parameters:
    ///   - topic: destination topic.
    ///   - partition: destination partition, or `nil` to let the partitioner choose.
    ///   - key: record key, or `nil` for a record with no key.
    ///   - value: record value, or `nil` for a tombstone.
    ///   - headers: record headers, in order.
    /// - Returns: the partition and offset the broker assigned.
    func produce(
        topic: String,
        partition: Int32?,
        key: Data?,
        value: Data?,
        headers: [RecordHeader]
    ) async throws -> DeliveryReport {
        try await client.produce(
            topic: topic,
            partition: partition,
            key: key,
            value: value,
            headers: headers
        )
    }

    // MARK: Avro

    /// Decodes a Confluent-framed Avro payload to pretty JSON.
    ///
    /// - Returns: the decoded record, or `nil` when the payload is not framed
    ///   and so is not Avro at all.
    /// - Throws: ``SchemaRegistryError/notConfigured`` when the payload is Avro
    ///   but the cluster has no registry, which the detail pane shows instead of
    ///   falling back to hex with no explanation.
    func decodeAvro(_ payload: Data?) async throws -> DecodedAvro? {
        if let registryProblem, AvroPayload.isFramed(payload) {
            throw SchemaRegistryError.badURL(registryProblem)
        }
        return try await AvroPayload.decode(payload, registry: registry)
    }

    /// The subjects the cluster's registry knows, for the produce sheet's picker.
    func subjects() async throws -> [String] {
        guard let registry else { throw SchemaRegistryError.notConfigured }
        return try await registry.subjects()
    }

    /// Encodes JSON as Avro against a subject's newest schema, framed for the
    /// wire.
    func encodeAvro(json: String, subject: String) async throws -> Data {
        guard let registry else { throw SchemaRegistryError.notConfigured }
        return try await AvroPayload.encode(json: json, subject: subject, registry: registry)
    }

    /// Scans records for a query, honouring cancellation of the calling task.
    ///
    /// - Parameters:
    ///   - query: what to look for.
    ///   - scopes: topics and partitions to read.
    ///   - progress: called from outside the main actor, hence `@Sendable`.
    func search(
        query: SearchQuery,
        scopes: [SearchScope],
        progress: (@Sendable (Int, Int) -> Void)? = nil
    ) async throws -> SearchOutcome {
        let consumer = try KafkaConsumer(profile: profile, secrets: secrets)
        return try await RecordSearcher(consumer: consumer).scan(
            query: query,
            scopes: scopes,
            progress: progress
        )
    }

    /// Generates records from templates.
    ///
    /// The work happens inside ``RecordGenerator``, an actor, so expanding
    /// templates and waiting on the broker stay off the main thread.
    ///
    /// - Parameters:
    ///   - request: what to generate.
    ///   - progress: called from outside the main actor, hence `@Sendable`.
    func generate(
        request: GenerateRequest,
        progress: (@Sendable (Int, Int) -> Void)? = nil
    ) async -> GenerateOutcome {
        await RecordGenerator(client: client).generate(request: request, progress: progress)
    }

    /// Counts the records an export request covers.
    func countForExport(request: ExportRequest) async throws -> Int {
        let consumer = try KafkaConsumer(profile: profile, secrets: secrets)
        return try await RecordExporter.plan(request: request, consumer: consumer).total
    }

    /// Exports records to `sink`, reporting progress as it goes.
    func export(
        request: ExportRequest,
        sink: ExportSink,
        progress: ((Int, Int) -> Void)? = nil
    ) async throws -> ExportOutcome {
        let consumer = try KafkaConsumer(profile: profile, secrets: secrets)
        return try await RecordExporter.export(
            request: request,
            consumer: consumer,
            sink: sink,
            progress: progress
        )
    }

    // MARK: Group offsets

    func offsets(forGroup group: String) -> [GroupPartitionOffset] {
        groupOffsetRows[group] ?? []
    }

    func offsetLoad(forGroup group: String) -> Load {
        groupOffsetLoads[group] ?? .notLoaded
    }

    /// Loads a group's committed offsets, log ends and lag.
    ///
    /// Lag is a snapshot: the committed offsets and the log ends are read in
    /// separate requests, so a busy partition can move in between.
    func loadOffsets(forGroup group: String) async {
        guard phase == .connected else { return }
        guard offsetLoad(forGroup: group) != .loading else { return }

        groupOffsetLoads[group] = .loading
        do {
            groupOffsetRows[group] = try await client.groupOffsets(group: group)
            groupOffsetLoads[group] = .loaded
        } catch {
            groupOffsetRows[group] = []
            groupOffsetLoads[group] = .failed(Self.message(for: error))
        }
    }

    /// Moves a group's committed offsets, then reloads them.
    ///
    /// - Important: Kafka refuses this while the group has active members, so
    ///   the consumers must be stopped first.
    func resetOffsets(
        group: String,
        positions: [(topic: String, partition: Int32, target: OffsetReset)]
    ) async throws {
        try await client.resetGroupOffsets(group: group, positions: positions)
        await loadOffsets(forGroup: group)
    }

    // MARK: Topic management

    /// Creates a topic, then refreshes metadata so the tree shows it.
    ///
    /// - Returns: `false` when the topic already existed.
    @discardableResult
    func createTopic(
        name: String,
        partitions: Int32,
        replicationFactor: Int32,
        configs: [String: String]
    ) async throws -> Bool {
        let created = try await client.createTopic(
            name: name,
            partitions: partitions,
            replicationFactor: replicationFactor,
            configs: configs
        )
        await refresh()
        return created
    }

    /// Deletes a topic and refreshes metadata.
    ///
    /// - Important: Irreversible; every record in the topic is discarded.
    func deleteTopic(name: String) async throws {
        try await client.deleteTopic(name: name)
        invalidateConfigs(for: .topic(name))
        await refresh()
    }

    /// Raises a topic's partition count and refreshes metadata.
    func createPartitions(topic: String, totalCount: Int) async throws {
        try await client.createPartitions(topic: topic, totalCount: totalCount)
        await refresh()
    }

    /// Deletes records below an offset and drops the cached page for that partition.
    ///
    /// - Returns: the partition's new low watermark.
    @discardableResult
    func deleteRecords(topic: String, partition: Int32, beforeOffset: Int64?) async throws -> Int64 {
        let low = try await client.deleteRecords(
            topic: topic,
            partition: partition,
            beforeOffset: beforeOffset
        )
        // The cached page may now point at deleted offsets.
        let key = PartitionKey(topic: topic, partition: partition)
        recordPages[key] = nil
        recordLoads[key] = .notLoaded
        return low
    }

    private func invalidateConfigs(for resource: ConfigResource) {
        configEntries[resource] = nil
        configLoads[resource] = nil
    }

    private func activeConsumer() throws -> KafkaConsumer {
        if let consumer { return consumer }
        let created = try KafkaConsumer(profile: profile, secrets: secrets)
        consumer = created
        return created
    }

    func broker(id: Int32) -> BrokerInfo? {
        brokers.first { $0.id == id }
    }

    func topic(named name: String) -> TopicInfo? {
        topics.first { $0.name == name }
    }

    func group(id: String) -> ConsumerGroupInfo? {
        groups.first { $0.id == id }
    }

    static func message(for error: Error) -> String {
        (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
    }
}
