import Crdkafka
import Foundation

/// Secrets a connection may need. The caller fetches these from the Keychain;
/// ``KafkaClient`` never reads them itself.
public struct ClusterSecrets: Sendable {
    public var saslPassword: String?
    public var tlsKeyPassphrase: String?
    /// Basic-auth password for the cluster's Schema Registry. Unused by the
    /// broker connection; carried here so one fetch covers every secret a
    /// cluster owns.
    public var schemaRegistryPassword: String?
    /// Basic-auth password for the cluster's Kafka Connect worker.
    public var connectPassword: String?

    public static let none = ClusterSecrets()

    public init(
        saslPassword: String? = nil,
        tlsKeyPassphrase: String? = nil,
        schemaRegistryPassword: String? = nil,
        connectPassword: String? = nil
    ) {
        self.saslPassword = saslPassword
        self.tlsKeyPassphrase = tlsKeyPassphrase
        self.schemaRegistryPassword = schemaRegistryPassword
        self.connectPassword = connectPassword
    }
}

/// Owns the `rd_kafka_t` handle and destroys it exactly once.
private final class KafkaHandle: @unchecked Sendable {
    let rk: OpaquePointer

    init(_ rk: OpaquePointer) {
        self.rk = rk
    }

    deinit {
        rd_kafka_destroy(rk)
    }
}

/// A connection to one Kafka cluster.
///
/// Every librdkafka call blocks, so the actor hands the work to a private serial
/// queue instead of blocking a cooperative thread. The actor itself serialises
/// access to the handle.
///
/// Instances are cheap to create but hold broker sockets; drop the client to
/// disconnect.
public actor KafkaClient {
    private let handle: KafkaHandle
    private let errors: KafkaDiagnosticsBox
    private let queue: DispatchQueue

    /// The librdkafka build in use, e.g. `2.15.1`.
    public static var librdkafkaVersion: String {
        String(cString: rd_kafka_version_str())
    }

    /// Builds a client for `profile`.
    ///
    /// - Parameters:
    ///   - profile: connection settings; only the fields the profile's security
    ///     protocol actually uses are applied.
    ///   - secrets: password and passphrase, if the protocol needs them.
    /// - Throws: ``KafkaError/configuration(key:reason:)`` for a rejected
    ///   setting, ``KafkaError/clientCreation(_:)`` if the handle cannot be made.
    ///   Creating a client does not contact the cluster, so an unreachable
    ///   broker fails later, on the first request.
    public init(profile: ClusterProfile, secrets: ClusterSecrets = .none) throws {
        let errors = KafkaDiagnosticsBox()
        let conf = try KafkaConfigurationBuilder.make(
            profile: profile,
            secrets: secrets,
            diagnostics: errors
        )

        var errstr = [CChar](repeating: 0, count: 512)
        guard let rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, &errstr, errstr.count) else {
            // rd_kafka_new only takes ownership of the config on success.
            rd_kafka_conf_destroy(conf)
            throw KafkaError.clientCreation(KafkaConfigurationBuilder.text(errstr))
        }

        self.errors = errors
        self.handle = KafkaHandle(rk)
        self.queue = DispatchQueue(label: "dev.kestrel.kafka.\(profile.id.uuidString)")
    }

    /// Runs a blocking librdkafka call off the cooperative thread pool.
    private func perform<T: Sendable>(
        _ body: @escaping @Sendable (OpaquePointer, KafkaDiagnosticsBox) throws -> T
    ) async throws -> T {
        let handle = self.handle
        let errors = self.errors
        return try await withCheckedThrowingContinuation { continuation in
            queue.async {
                do {
                    continuation.resume(returning: try body(handle.rk, errors))
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    /// Fetches cluster metadata.
    ///
    /// - Parameters:
    ///   - allTopics: `true` for every topic in the cluster, `false` for only
    ///     topics this client already knows about.
    ///   - timeout: how long to wait for a broker to answer.
    /// - Throws: ``KafkaError/unreachable(detail:)`` when no broker answers.
    public func metadata(
        allTopics: Bool = true,
        timeout: Duration = .seconds(10)
    ) async throws -> ClusterMetadata {
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000

        return try await perform { rk, errors in
            var pointer: UnsafePointer<rd_kafka_metadata>?
            let code = rd_kafka_metadata(rk, allTopics ? 1 : 0, nil, &pointer, timeoutMs)

            guard code == RD_KAFKA_RESP_ERR_NO_ERROR, let raw = pointer else {
                // Let queued events land so the error callback can explain why.
                rd_kafka_poll(rk, 200)
                throw KafkaError.from(code: code, timeout: timeoutSeconds, detail: errors.take())
            }
            defer { rd_kafka_metadata_destroy(raw) }
            return Self.convert(raw.pointee)
        }
    }

    /// Lists topic names, sorted, optionally hiding internal `__` topics.
    public func topicNames(
        includeInternal: Bool = false,
        timeout: Duration = .seconds(10)
    ) async throws -> [String] {
        let metadata = try await metadata(timeout: timeout)
        return metadata.topics
            .filter { includeInternal || !$0.isInternal }
            .map(\.name)
            .sorted()
    }

    /// Lists consumer groups known to the cluster.
    ///
    /// Uses librdkafka's `rd_kafka_list_groups`, which reports the group
    /// coordinator's view. Offsets and lag come later, in slice 08.
    public func consumerGroups(timeout: Duration = .seconds(10)) async throws -> [ConsumerGroupInfo] {
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000

        return try await perform { rk, errors in
            var pointer: UnsafePointer<rd_kafka_group_list>?
            let code = rd_kafka_list_groups(rk, nil, &pointer, timeoutMs)

            guard code == RD_KAFKA_RESP_ERR_NO_ERROR, let raw = pointer else {
                rd_kafka_poll(rk, 200)
                throw KafkaError.from(code: code, timeout: timeoutSeconds, detail: errors.take())
            }
            defer { rd_kafka_group_list_destroy(raw) }
            return Self.convert(raw.pointee)
        }
    }

    /// Reads the configuration of one broker or topic (DescribeConfigs).
    ///
    /// Entries come back sorted by name. Sensitive entries arrive with a `nil`
    /// value because brokers refuse to disclose them.
    ///
    /// - Parameters:
    ///   - resource: the broker id or topic name to describe.
    ///   - timeout: how long to wait for the admin response.
    /// - Throws: ``KafkaError/broker(code:name:detail:)`` when the cluster
    ///   reports an error for the resource, or ``KafkaError/timedOut(seconds:)``
    ///   when nothing arrives in time. Requires broker version 0.11 or newer.
    public func describeConfigs(
        _ resource: ConfigResource,
        timeout: Duration = .seconds(10)
    ) async throws -> [ConfigEntry] {
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000
        let resourceType: rd_kafka_ResourceType_t = switch resource {
        case .broker: RD_KAFKA_RESOURCE_BROKER
        case .topic: RD_KAFKA_RESOURCE_TOPIC
        }
        let resourceName = resource.name

        return try await perform { rk, errors in
            guard let queue = rd_kafka_queue_new(rk) else {
                throw KafkaError.clientCreation("rd_kafka_queue_new returned null")
            }
            defer { rd_kafka_queue_destroy(queue) }

            guard let options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS) else {
                throw KafkaError.clientCreation("rd_kafka_AdminOptions_new returned null")
            }
            defer { rd_kafka_AdminOptions_destroy(options) }

            var errstr = [CChar](repeating: 0, count: 512)
            _ = rd_kafka_AdminOptions_set_request_timeout(options, timeoutMs, &errstr, errstr.count)

            guard let configResource = rd_kafka_ConfigResource_new(resourceType, resourceName) else {
                throw KafkaError.configuration(key: resourceName, reason: "unsupported resource")
            }
            // librdkafka copies the request, so the array stays ours to free.
            var resources: [OpaquePointer?] = [configResource]
            defer { rd_kafka_ConfigResource_destroy_array(&resources, 1) }

            rd_kafka_DescribeConfigs(rk, &resources, 1, options, queue)

            // The admin call is asynchronous; its reply arrives as an event.
            guard let event = rd_kafka_queue_poll(queue, timeoutMs) else {
                throw KafkaError.timedOut(seconds: timeoutSeconds)
            }
            defer { rd_kafka_event_destroy(event) }

            let eventError = rd_kafka_event_error(event)
            if eventError != RD_KAFKA_RESP_ERR_NO_ERROR {
                let detail = rd_kafka_event_error_string(event).map { String(cString: $0) }
                    ?? errors.take()
                throw KafkaError.from(code: eventError, timeout: timeoutSeconds, detail: detail)
            }

            guard let result = rd_kafka_event_DescribeConfigs_result(event) else {
                throw KafkaError.broker(
                    code: 0,
                    name: "DescribeConfigs",
                    detail: "the cluster returned an unexpected event"
                )
            }

            var resourceCount = 0
            guard let described = rd_kafka_DescribeConfigs_result_resources(result, &resourceCount),
                  resourceCount > 0,
                  let first = described[0]
            else {
                return []
            }

            let resourceError = rd_kafka_ConfigResource_error(first)
            if resourceError != RD_KAFKA_RESP_ERR_NO_ERROR {
                let detail = rd_kafka_ConfigResource_error_string(first).map { String(cString: $0) } ?? ""
                throw KafkaError.from(code: resourceError, timeout: timeoutSeconds, detail: detail)
            }

            var entryCount = 0
            guard let entries = rd_kafka_ConfigResource_configs(first, &entryCount) else { return [] }

            var configs: [ConfigEntry] = []
            for index in 0..<entryCount {
                guard let entry = entries[index] else { continue }
                guard let namePointer = rd_kafka_ConfigEntry_name(entry) else { continue }
                configs.append(
                    ConfigEntry(
                        name: String(cString: namePointer),
                        value: rd_kafka_ConfigEntry_value(entry).map { String(cString: $0) },
                        isDefault: rd_kafka_ConfigEntry_is_default(entry) == 1,
                        isReadOnly: rd_kafka_ConfigEntry_is_read_only(entry) == 1,
                        isSensitive: rd_kafka_ConfigEntry_is_sensitive(entry) == 1
                    )
                )
            }
            return configs.sorted { $0.name < $1.name }
        }
    }

    /// Contacts the cluster and reports what it found, without throwing.
    ///
    /// Intended for the "Test Connection" button: a failure is a message to
    /// show, not an error to handle.
    // MARK: Topic management

    /// Runs one admin request and returns whatever the caller reads off the event.
    ///
    /// Every admin call in librdkafka follows the same shape: make a queue and
    /// an options object, submit a request, poll for exactly one event, check
    /// the event-level error, then read the operation's own result. This wraps
    /// that so each operation only writes the parts that differ.
    ///
    /// Must be called on the client's private queue, i.e. inside ``perform``.
    ///
    /// - Parameters:
    ///   - submit: enqueues the request. Receives the options and the queue.
    ///   - read: reads the result from the event, which stays valid only for
    ///     the duration of the call.
    private static func runAdmin<T>(
        rk: OpaquePointer,
        errors: KafkaDiagnosticsBox,
        operation: rd_kafka_admin_op_t,
        timeoutMs: Int32,
        timeoutSeconds: Double,
        submit: (OpaquePointer, OpaquePointer) throws -> Void,
        read: (OpaquePointer) throws -> T
    ) throws -> T {
        guard let queue = rd_kafka_queue_new(rk) else {
            throw KafkaError.clientCreation("rd_kafka_queue_new returned null")
        }
        defer { rd_kafka_queue_destroy(queue) }

        guard let options = rd_kafka_AdminOptions_new(rk, operation) else {
            throw KafkaError.clientCreation("rd_kafka_AdminOptions_new returned null")
        }
        defer { rd_kafka_AdminOptions_destroy(options) }

        var errstr = [CChar](repeating: 0, count: 512)
        _ = rd_kafka_AdminOptions_set_request_timeout(options, timeoutMs, &errstr, errstr.count)
        // How long the brokers themselves may take; topic creation and record
        // deletion both need this, not just the client-side request timeout.
        _ = rd_kafka_AdminOptions_set_operation_timeout(options, timeoutMs, &errstr, errstr.count)

        try submit(options, queue)

        guard let event = rd_kafka_queue_poll(queue, timeoutMs) else {
            throw KafkaError.timedOut(seconds: timeoutSeconds)
        }
        defer { rd_kafka_event_destroy(event) }

        let eventError = rd_kafka_event_error(event)
        if eventError != RD_KAFKA_RESP_ERR_NO_ERROR {
            let detail = rd_kafka_event_error_string(event).map { String(cString: $0) }
                ?? errors.take()
            throw KafkaError.from(code: eventError, timeout: timeoutSeconds, detail: detail)
        }

        return try read(event)
    }

    /// Throws if a per-topic result carries an error.
    private static func check(
        topicResult: OpaquePointer?,
        timeoutSeconds: Double
    ) throws {
        guard let topicResult else { return }
        let code = rd_kafka_topic_result_error(topicResult)
        guard code != RD_KAFKA_RESP_ERR_NO_ERROR else { return }
        throw KafkaError.from(
            code: code,
            timeout: timeoutSeconds,
            detail: rd_kafka_topic_result_error_string(topicResult).map { String(cString: $0) }
        )
    }

    /// Deletes a topic and all of its data.
    ///
    /// - Important: Irreversible. The broker must have `delete.topic.enable`
    ///   set, which is the default; otherwise the call fails.
    ///
    /// - Parameter name: topic to delete.
    public func deleteTopic(name: String, timeout: Duration = .seconds(30)) async throws {
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000

        try await perform { rk, errors in
            guard let request = rd_kafka_DeleteTopic_new(name) else {
                throw KafkaError.configuration(key: name, reason: "invalid topic name")
            }
            var requests: [OpaquePointer?] = [request]
            defer { rd_kafka_DeleteTopic_destroy_array(&requests, 1) }

            try Self.runAdmin(
                rk: rk,
                errors: errors,
                operation: RD_KAFKA_ADMIN_OP_DELETETOPICS,
                timeoutMs: timeoutMs,
                timeoutSeconds: timeoutSeconds,
                submit: { options, queue in
                    rd_kafka_DeleteTopics(rk, &requests, 1, options, queue)
                },
                read: { event in
                    guard let result = rd_kafka_event_DeleteTopics_result(event) else {
                        throw KafkaError.broker(
                            code: 0,
                            name: "DeleteTopics",
                            detail: "the cluster returned an unexpected event"
                        )
                    }
                    var count = 0
                    guard let topics = rd_kafka_DeleteTopics_result_topics(result, &count),
                          count > 0 else { return }
                    try Self.check(topicResult: topics[0], timeoutSeconds: timeoutSeconds)
                }
            )
        }
    }

    /// Raises a topic's partition count.
    ///
    /// - Important: Kafka can only add partitions, never remove them, and
    ///   adding them changes which partition a key hashes to. Existing records
    ///   are not moved.
    ///
    /// - Parameters:
    ///   - topic: topic to widen.
    ///   - totalCount: the new **total** partition count, not how many to add.
    ///     It must be greater than the current count.
    public func createPartitions(
        topic: String,
        totalCount: Int,
        timeout: Duration = .seconds(30)
    ) async throws {
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000

        try await perform { rk, errors in
            var errstr = [CChar](repeating: 0, count: 512)
            guard let request = rd_kafka_NewPartitions_new(
                topic,
                totalCount,
                &errstr,
                errstr.count
            ) else {
                throw KafkaError.configuration(
                    key: topic,
                    reason: KafkaConfigurationBuilder.text(errstr)
                )
            }
            var requests: [OpaquePointer?] = [request]
            defer { rd_kafka_NewPartitions_destroy_array(&requests, 1) }

            try Self.runAdmin(
                rk: rk,
                errors: errors,
                operation: RD_KAFKA_ADMIN_OP_CREATEPARTITIONS,
                timeoutMs: timeoutMs,
                timeoutSeconds: timeoutSeconds,
                submit: { options, queue in
                    rd_kafka_CreatePartitions(rk, &requests, 1, options, queue)
                },
                read: { event in
                    guard let result = rd_kafka_event_CreatePartitions_result(event) else {
                        throw KafkaError.broker(
                            code: 0,
                            name: "CreatePartitions",
                            detail: "the cluster returned an unexpected event"
                        )
                    }
                    var count = 0
                    guard let topics = rd_kafka_CreatePartitions_result_topics(result, &count),
                          count > 0 else { return }
                    try Self.check(topicResult: topics[0], timeoutSeconds: timeoutSeconds)
                }
            )
        }
    }

    /// Deletes the records before an offset, trimming the start of a partition.
    ///
    /// This moves the low watermark; it does not compact or rewrite anything.
    /// Only whole log segments are reclaimed, so the low watermark the broker
    /// reports back can be lower than requested.
    ///
    /// - Parameters:
    ///   - topic: topic to trim.
    ///   - partition: partition to trim.
    ///   - beforeOffset: records below this offset are deleted. Pass `nil` to
    ///     delete every record currently in the partition.
    /// - Returns: the partition's new low watermark.
    @discardableResult
    public func deleteRecords(
        topic: String,
        partition: Int32,
        beforeOffset: Int64?,
        timeout: Duration = .seconds(30)
    ) async throws -> Int64 {
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000

        return try await perform { rk, errors in
            guard let list = rd_kafka_topic_partition_list_new(1) else {
                throw KafkaError.clientCreation("rd_kafka_topic_partition_list_new returned null")
            }
            defer { rd_kafka_topic_partition_list_destroy(list) }

            guard let entry = rd_kafka_topic_partition_list_add(list, topic, partition) else {
                throw KafkaError.configuration(key: topic, reason: "could not add the partition")
            }
            // RD_KAFKA_OFFSET_END means "everything currently in the partition".
            entry.pointee.offset = beforeOffset ?? Int64(RD_KAFKA_OFFSET_END)

            guard let request = rd_kafka_DeleteRecords_new(list) else {
                throw KafkaError.configuration(key: topic, reason: "could not build the request")
            }
            var requests: [OpaquePointer?] = [request]
            defer { rd_kafka_DeleteRecords_destroy_array(&requests, 1) }

            return try Self.runAdmin(
                rk: rk,
                errors: errors,
                operation: RD_KAFKA_ADMIN_OP_DELETERECORDS,
                timeoutMs: timeoutMs,
                timeoutSeconds: timeoutSeconds,
                submit: { options, queue in
                    rd_kafka_DeleteRecords(rk, &requests, 1, options, queue)
                },
                read: { event in
                    guard let result = rd_kafka_event_DeleteRecords_result(event) else {
                        throw KafkaError.broker(
                            code: 0,
                            name: "DeleteRecords",
                            detail: "the cluster returned an unexpected event"
                        )
                    }
                    guard let offsets = rd_kafka_DeleteRecords_result_offsets(result),
                          offsets.pointee.cnt > 0 else {
                        return 0
                    }
                    let first = offsets.pointee.elems[0]
                    if first.err != RD_KAFKA_RESP_ERR_NO_ERROR {
                        throw KafkaError.from(
                            code: first.err,
                            timeout: timeoutSeconds,
                            detail: "\(topic) [\(partition)]"
                        )
                    }
                    return first.offset
                }
            )
        }
    }

    // MARK: Consumer group offsets

    /// Lists a group's committed offsets with the matching log end and lag.
    ///
    /// Uses the ListConsumerGroupOffsets admin API, so it neither joins the
    /// group nor disturbs its members. The log end for each partition is then
    /// queried separately, which means lag is a snapshot: a busy partition can
    /// advance between the two calls.
    ///
    /// - Parameter group: group id.
    /// - Returns: one entry per partition the group has committed, sorted by
    ///   topic then partition.
    public func groupOffsets(
        group: String,
        timeout: Duration = .seconds(15)
    ) async throws -> [GroupPartitionOffset] {
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000

        return try await perform { rk, errors in
            guard let queue = rd_kafka_queue_new(rk) else {
                throw KafkaError.clientCreation("rd_kafka_queue_new returned null")
            }
            defer { rd_kafka_queue_destroy(queue) }

            guard let options = rd_kafka_AdminOptions_new(
                rk,
                RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS
            ) else {
                throw KafkaError.clientCreation("rd_kafka_AdminOptions_new returned null")
            }
            defer { rd_kafka_AdminOptions_destroy(options) }

            var errstr = [CChar](repeating: 0, count: 512)
            _ = rd_kafka_AdminOptions_set_request_timeout(options, timeoutMs, &errstr, errstr.count)

            // A nil partition list asks for every partition the group has
            // committed, which is what a group inspector wants.
            guard let request = rd_kafka_ListConsumerGroupOffsets_new(group, nil) else {
                throw KafkaError.configuration(key: group, reason: "could not build the request")
            }
            var requests: [OpaquePointer?] = [request]
            defer { rd_kafka_ListConsumerGroupOffsets_destroy(request) }

            rd_kafka_ListConsumerGroupOffsets(rk, &requests, 1, options, queue)

            guard let event = rd_kafka_queue_poll(queue, timeoutMs) else {
                throw KafkaError.timedOut(seconds: timeoutSeconds)
            }
            defer { rd_kafka_event_destroy(event) }

            let eventError = rd_kafka_event_error(event)
            if eventError != RD_KAFKA_RESP_ERR_NO_ERROR {
                let detail = rd_kafka_event_error_string(event).map { String(cString: $0) }
                    ?? errors.take()
                throw KafkaError.from(code: eventError, timeout: timeoutSeconds, detail: detail)
            }

            guard let result = rd_kafka_event_ListConsumerGroupOffsets_result(event) else {
                throw KafkaError.broker(
                    code: 0,
                    name: "ListConsumerGroupOffsets",
                    detail: "the cluster returned an unexpected event"
                )
            }

            var groupCount = 0
            guard let groups = rd_kafka_ListConsumerGroupOffsets_result_groups(result, &groupCount),
                  groupCount > 0 else {
                return []
            }

            let groupResult = groups[0]
            if let error = rd_kafka_group_result_error(groupResult),
               rd_kafka_error_code(error) != RD_KAFKA_RESP_ERR_NO_ERROR {
                throw KafkaError.from(
                    code: rd_kafka_error_code(error),
                    timeout: timeoutSeconds,
                    detail: rd_kafka_error_string(error).map { String(cString: $0) }
                )
            }

            guard let partitions = rd_kafka_group_result_partitions(groupResult) else { return [] }

            var offsets: [GroupPartitionOffset] = []
            for index in 0..<Int(partitions.pointee.cnt) {
                let entry = partitions.pointee.elems[index]
                let topic = entry.topic.map { String(cString: $0) } ?? ""

                var low: Int64 = 0
                var high: Int64 = 0
                let watermarkCode = rd_kafka_query_watermark_offsets(
                    rk, topic, entry.partition, &low, &high, timeoutMs
                )

                offsets.append(
                    GroupPartitionOffset(
                        topic: topic,
                        partition: entry.partition,
                        // RD_KAFKA_OFFSET_INVALID means never committed.
                        committed: entry.offset >= 0 ? entry.offset : nil,
                        logEnd: watermarkCode == RD_KAFKA_RESP_ERR_NO_ERROR ? high : 0,
                        error: entry.err == RD_KAFKA_RESP_ERR_NO_ERROR
                            ? nil
                            : String(cString: rd_kafka_err2str(entry.err))
                    )
                )
            }

            return offsets.sorted {
                ($0.topic, $0.partition) < ($1.topic, $1.partition)
            }
        }
    }

    /// Moves a group's committed offsets.
    ///
    /// - Important: Kafka rejects this while the group has active members, with
    ///   an "unknown member" or "non-empty group" error. Stop the consumers
    ///   first; the caller is expected to confirm this with the user.
    ///
    /// - Parameters:
    ///   - group: group id.
    ///   - positions: partitions to move, paired with the target position.
    ///     `.earliest` and `.latest` resolve against each partition's current
    ///     watermarks before the request is sent.
    /// - Returns: the offsets the broker accepted, in the order it reported.
    @discardableResult
    public func resetGroupOffsets(
        group: String,
        positions: [(topic: String, partition: Int32, target: OffsetReset)],
        timeout: Duration = .seconds(20)
    ) async throws -> [GroupPartitionOffset] {
        guard !positions.isEmpty else { return [] }
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000

        return try await perform { rk, errors in
            guard let list = rd_kafka_topic_partition_list_new(Int32(positions.count)) else {
                throw KafkaError.clientCreation("rd_kafka_topic_partition_list_new returned null")
            }
            defer { rd_kafka_topic_partition_list_destroy(list) }

            for position in positions {
                guard let entry = rd_kafka_topic_partition_list_add(
                    list,
                    position.topic,
                    position.partition
                ) else {
                    throw KafkaError.configuration(
                        key: position.topic,
                        reason: "could not add partition \(position.partition)"
                    )
                }

                switch position.target {
                case .offset(let offset):
                    entry.pointee.offset = offset
                case .earliest, .latest:
                    // AlterConsumerGroupOffsets needs a concrete offset, so the
                    // watermarks are resolved here rather than by the broker.
                    var low: Int64 = 0
                    var high: Int64 = 0
                    let code = rd_kafka_query_watermark_offsets(
                        rk, position.topic, position.partition, &low, &high, timeoutMs
                    )
                    guard code == RD_KAFKA_RESP_ERR_NO_ERROR else {
                        throw KafkaError.from(
                            code: code,
                            timeout: timeoutSeconds,
                            detail: errors.take()
                        )
                    }
                    entry.pointee.offset = position.target == .earliest ? low : high
                }
            }

            guard let queue = rd_kafka_queue_new(rk) else {
                throw KafkaError.clientCreation("rd_kafka_queue_new returned null")
            }
            defer { rd_kafka_queue_destroy(queue) }

            guard let options = rd_kafka_AdminOptions_new(
                rk,
                RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS
            ) else {
                throw KafkaError.clientCreation("rd_kafka_AdminOptions_new returned null")
            }
            defer { rd_kafka_AdminOptions_destroy(options) }

            var errstr = [CChar](repeating: 0, count: 512)
            _ = rd_kafka_AdminOptions_set_request_timeout(options, timeoutMs, &errstr, errstr.count)

            guard let request = rd_kafka_AlterConsumerGroupOffsets_new(group, list) else {
                throw KafkaError.configuration(key: group, reason: "could not build the request")
            }
            var requests: [OpaquePointer?] = [request]
            defer { rd_kafka_AlterConsumerGroupOffsets_destroy(request) }

            rd_kafka_AlterConsumerGroupOffsets(rk, &requests, 1, options, queue)

            guard let event = rd_kafka_queue_poll(queue, timeoutMs) else {
                throw KafkaError.timedOut(seconds: timeoutSeconds)
            }
            defer { rd_kafka_event_destroy(event) }

            let eventError = rd_kafka_event_error(event)
            if eventError != RD_KAFKA_RESP_ERR_NO_ERROR {
                let detail = rd_kafka_event_error_string(event).map { String(cString: $0) }
                    ?? errors.take()
                throw KafkaError.from(code: eventError, timeout: timeoutSeconds, detail: detail)
            }

            guard let result = rd_kafka_event_AlterConsumerGroupOffsets_result(event) else {
                throw KafkaError.broker(
                    code: 0,
                    name: "AlterConsumerGroupOffsets",
                    detail: "the cluster returned an unexpected event"
                )
            }

            var groupCount = 0
            guard let groups = rd_kafka_AlterConsumerGroupOffsets_result_groups(result, &groupCount),
                  groupCount > 0 else {
                return []
            }

            let groupResult = groups[0]
            if let error = rd_kafka_group_result_error(groupResult),
               rd_kafka_error_code(error) != RD_KAFKA_RESP_ERR_NO_ERROR {
                throw KafkaError.from(
                    code: rd_kafka_error_code(error),
                    timeout: timeoutSeconds,
                    detail: rd_kafka_error_string(error).map { String(cString: $0) }
                )
            }

            guard let partitions = rd_kafka_group_result_partitions(groupResult) else { return [] }

            var applied: [GroupPartitionOffset] = []
            for index in 0..<Int(partitions.pointee.cnt) {
                let entry = partitions.pointee.elems[index]
                // A per-partition error here means that partition was refused.
                if entry.err != RD_KAFKA_RESP_ERR_NO_ERROR {
                    throw KafkaError.from(
                        code: entry.err,
                        timeout: timeoutSeconds,
                        detail: entry.topic.map { "\(String(cString: $0)) [\(entry.partition)]" }
                    )
                }
                applied.append(
                    GroupPartitionOffset(
                        topic: entry.topic.map { String(cString: $0) } ?? "",
                        partition: entry.partition,
                        committed: entry.offset,
                        logEnd: entry.offset
                    )
                )
            }
            return applied
        }
    }

    // MARK: Producing

    /// Sends one record and waits for its delivery report.
    ///
    /// - Parameters:
    ///   - topic: destination topic. It must already exist unless the broker
    ///     auto-creates topics; a missing topic surfaces as
    ///     ``KafkaError/broker(code:name:detail:)`` for unknown topic.
    ///   - partition: destination partition, or `nil` to let the configured
    ///     partitioner choose (by key hash when a key is present).
    ///   - key: record key. `nil` produces a record with no key.
    ///   - value: record value. `nil` produces a tombstone.
    ///   - headers: record headers, in order. Duplicate names are allowed.
    ///   - timeout: how long to wait for the delivery report.
    /// - Returns: the partition and offset the broker assigned.
    public func produce(
        topic: String,
        partition: Int32? = nil,
        key: Data?,
        value: Data?,
        headers: [RecordHeader] = [],
        timeout: Duration = .seconds(10)
    ) async throws -> DeliveryReport {
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000

        return try await perform { rk, errors in
            let box = DeliveryBox()
            // Retained across the C call; the delivery callback releases it.
            let opaque = Unmanaged.passRetained(box).toOpaque()

            // Copies owned by this scope. librdkafka copies key and value
            // because of RD_KAFKA_MSG_F_COPY, and copies the topic name, so
            // everything here can be freed as soon as produceva returns.
            let topicName = strdup(topic)
            defer { free(topicName) }
            var keyBytes = key.map { [UInt8]($0) } ?? []
            var valueBytes = value.map { [UInt8]($0) } ?? []

            var built: OpaquePointer?
            if !headers.isEmpty {
                guard let list = rd_kafka_headers_new(headers.count) else {
                    Unmanaged<DeliveryBox>.fromOpaque(opaque).release()
                    throw KafkaError.clientCreation("rd_kafka_headers_new returned null")
                }
                for header in headers {
                    let bytes = header.value.map { [UInt8]($0) } ?? []
                    let code = bytes.withUnsafeBytes { raw in
                        rd_kafka_header_add(
                            list,
                            header.name,
                            -1,
                            header.value == nil ? nil : raw.baseAddress,
                            bytes.count
                        )
                    }
                    if code != RD_KAFKA_RESP_ERR_NO_ERROR {
                        rd_kafka_headers_destroy(list)
                        Unmanaged<DeliveryBox>.fromOpaque(opaque).release()
                        throw KafkaError.from(code: code, timeout: timeoutSeconds, detail: nil)
                    }
                }
                built = list
            }

            let code: rd_kafka_resp_err_t = keyBytes.withUnsafeMutableBytes { keyRaw in
                valueBytes.withUnsafeMutableBytes { valueRaw in
                    var arguments: [rd_kafka_vu_t] = []

                    var topicArgument = rd_kafka_vu_t()
                    topicArgument.vtype = RD_KAFKA_VTYPE_TOPIC
                    topicArgument.u.cstr = UnsafePointer(topicName)
                    arguments.append(topicArgument)

                    var partitionArgument = rd_kafka_vu_t()
                    partitionArgument.vtype = RD_KAFKA_VTYPE_PARTITION
                    partitionArgument.u.i32 = partition ?? RD_KAFKA_PARTITION_UA
                    arguments.append(partitionArgument)

                    // Copy the payloads so librdkafka does not reference this
                    // stack once produceva returns.
                    var flags = rd_kafka_vu_t()
                    flags.vtype = RD_KAFKA_VTYPE_MSGFLAGS
                    flags.u.i = Int32(RD_KAFKA_MSG_F_COPY)
                    arguments.append(flags)

                    if key != nil {
                        var keyArgument = rd_kafka_vu_t()
                        keyArgument.vtype = RD_KAFKA_VTYPE_KEY
                        keyArgument.u.mem.ptr = keyRaw.baseAddress
                        keyArgument.u.mem.size = keyRaw.count
                        arguments.append(keyArgument)
                    }

                    // A nil value is a tombstone, which is not the same as
                    // empty bytes, so the argument is omitted entirely.
                    if value != nil {
                        var valueArgument = rd_kafka_vu_t()
                        valueArgument.vtype = RD_KAFKA_VTYPE_VALUE
                        valueArgument.u.mem.ptr = valueRaw.baseAddress
                        valueArgument.u.mem.size = valueRaw.count
                        arguments.append(valueArgument)
                    }

                    if let built {
                        var headerArgument = rd_kafka_vu_t()
                        headerArgument.vtype = RD_KAFKA_VTYPE_HEADERS
                        headerArgument.u.headers = built
                        arguments.append(headerArgument)
                    }

                    var opaqueArgument = rd_kafka_vu_t()
                    opaqueArgument.vtype = RD_KAFKA_VTYPE_OPAQUE
                    opaqueArgument.u.ptr = opaque
                    arguments.append(opaqueArgument)

                    guard let error = rd_kafka_produceva(rk, arguments, arguments.count) else {
                        return RD_KAFKA_RESP_ERR_NO_ERROR
                    }
                    defer { rd_kafka_error_destroy(error) }
                    return rd_kafka_error_code(error)
                }
            }

            guard code == RD_KAFKA_RESP_ERR_NO_ERROR else {
                // produceva only takes ownership of the headers on success.
                if let built { rd_kafka_headers_destroy(built) }
                Unmanaged<DeliveryBox>.fromOpaque(opaque).release()
                throw KafkaError.from(code: code, timeout: timeoutSeconds, detail: errors.take())
            }

            // flush serves the delivery report queue, which is what invokes
            // the callback that fills the box.
            rd_kafka_flush(rk, timeoutMs)

            guard let outcome = box.result else {
                throw KafkaError.timedOut(seconds: timeoutSeconds)
            }
            guard outcome.code == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.from(
                    code: outcome.code,
                    timeout: timeoutSeconds,
                    detail: errors.take()
                )
            }
            return DeliveryReport(partition: outcome.partition, offset: outcome.offset)
        }
    }

    /// Creates a topic, reporting whether it had to be created.
    ///
    /// An already-existing topic is not an error; the call reports
    /// `created: false` so callers can use this to ensure a topic exists.
    ///
    /// - Parameters:
    ///   - name: topic name.
    ///   - partitions: partition count for a new topic.
    ///   - replicationFactor: replication factor for a new topic. It cannot
    ///     exceed the number of brokers in the cluster.
    ///   - configs: topic configuration overrides, for example
    ///     `["cleanup.policy": "compact"]`. Ignored when the topic exists.
    @discardableResult
    public func createTopic(
        name: String,
        partitions: Int32 = 1,
        replicationFactor: Int32 = 1,
        configs: [String: String] = [:],
        timeout: Duration = .seconds(20)
    ) async throws -> Bool {
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000

        return try await perform { rk, errors in
            guard let queue = rd_kafka_queue_new(rk) else {
                throw KafkaError.clientCreation("rd_kafka_queue_new returned null")
            }
            defer { rd_kafka_queue_destroy(queue) }

            guard let options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS) else {
                throw KafkaError.clientCreation("rd_kafka_AdminOptions_new returned null")
            }
            defer { rd_kafka_AdminOptions_destroy(options) }

            var errstr = [CChar](repeating: 0, count: 512)
            _ = rd_kafka_AdminOptions_set_request_timeout(options, timeoutMs, &errstr, errstr.count)

            guard let newTopic = rd_kafka_NewTopic_new(
                name,
                partitions,
                replicationFactor,
                &errstr,
                errstr.count
            ) else {
                throw KafkaError.configuration(
                    key: name,
                    reason: KafkaConfigurationBuilder.text(errstr)
                )
            }
            var topics: [OpaquePointer?] = [newTopic]
            defer { rd_kafka_NewTopic_destroy_array(&topics, 1) }

            for (key, value) in configs {
                let code = rd_kafka_NewTopic_set_config(newTopic, key, value)
                guard code == RD_KAFKA_RESP_ERR_NO_ERROR else {
                    throw KafkaError.configuration(
                        key: key,
                        reason: String(cString: rd_kafka_err2str(code))
                    )
                }
            }

            rd_kafka_CreateTopics(rk, &topics, 1, options, queue)

            guard let event = rd_kafka_queue_poll(queue, timeoutMs) else {
                throw KafkaError.timedOut(seconds: timeoutSeconds)
            }
            defer { rd_kafka_event_destroy(event) }

            let eventError = rd_kafka_event_error(event)
            if eventError != RD_KAFKA_RESP_ERR_NO_ERROR {
                let detail = rd_kafka_event_error_string(event).map { String(cString: $0) }
                    ?? errors.take()
                throw KafkaError.from(code: eventError, timeout: timeoutSeconds, detail: detail)
            }

            guard let result = rd_kafka_event_CreateTopics_result(event) else {
                throw KafkaError.broker(
                    code: 0,
                    name: "CreateTopics",
                    detail: "the cluster returned an unexpected event"
                )
            }

            var count = 0
            guard let results = rd_kafka_CreateTopics_result_topics(result, &count), count > 0 else {
                return false
            }

            let topicError = rd_kafka_topic_result_error(results[0])
            switch topicError {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return true
            case RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS:
                return false
            default:
                let detail = rd_kafka_topic_result_error_string(results[0])
                    .map { String(cString: $0) }
                throw KafkaError.from(code: topicError, timeout: timeoutSeconds, detail: detail)
            }
        }
    }

    public func testConnection(timeout: Duration = .seconds(10)) async -> ConnectionTestResult {
        do {
            let metadata = try await metadata(allTopics: true, timeout: timeout)
            return .success(brokers: metadata.brokers, topicCount: metadata.topics.count)
        } catch {
            let message = (error as? LocalizedError)?.errorDescription ?? error.localizedDescription
            return .failure(message)
        }
    }

    // MARK: C struct conversion

    private static func convert(_ raw: rd_kafka_metadata) -> ClusterMetadata {
        var brokers: [BrokerInfo] = []
        for index in 0..<Int(raw.broker_cnt) {
            let broker = raw.brokers[index]
            brokers.append(
                BrokerInfo(
                    id: broker.id,
                    host: broker.host.map { String(cString: $0) } ?? "unknown",
                    port: broker.port
                )
            )
        }

        var topics: [TopicInfo] = []
        for index in 0..<Int(raw.topic_cnt) {
            let topic = raw.topics[index]
            var partitions: [PartitionInfo] = []
            for partitionIndex in 0..<Int(topic.partition_cnt) {
                let partition = topic.partitions[partitionIndex]
                partitions.append(
                    PartitionInfo(
                        id: partition.id,
                        leader: partition.leader,
                        replicas: Array(UnsafeBufferPointer(start: partition.replicas, count: Int(partition.replica_cnt))),
                        inSyncReplicas: Array(UnsafeBufferPointer(start: partition.isrs, count: Int(partition.isr_cnt)))
                    )
                )
            }
            topics.append(
                TopicInfo(
                    name: topic.topic.map { String(cString: $0) } ?? "unknown",
                    partitions: partitions.sorted { $0.id < $1.id },
                    error: topic.err == RD_KAFKA_RESP_ERR_NO_ERROR
                        ? nil
                        : String(cString: rd_kafka_err2str(topic.err))
                )
            )
        }

        let originating = raw.orig_broker_name.map { String(cString: $0) } ?? ""
        return ClusterMetadata(brokers: brokers, topics: topics, originatingBroker: originating)
    }

    private static func convert(_ raw: rd_kafka_group_list) -> [ConsumerGroupInfo] {
        var groups: [ConsumerGroupInfo] = []
        for index in 0..<Int(raw.group_cnt) {
            let group = raw.groups[index]
            var members: [ConsumerGroupMember] = []
            for memberIndex in 0..<Int(group.member_cnt) {
                let member = group.members[memberIndex]
                members.append(
                    ConsumerGroupMember(
                        id: member.member_id.map { String(cString: $0) } ?? "",
                        clientId: member.client_id.map { String(cString: $0) } ?? "",
                        clientHost: member.client_host.map { String(cString: $0) } ?? "",
                        assignments: member.member_assignment.map {
                            AssignmentDecoder.decode(
                                Data(bytes: $0, count: Int(member.member_assignment_size))
                            )
                        } ?? []
                    )
                )
            }
            groups.append(
                ConsumerGroupInfo(
                    id: group.group.map { String(cString: $0) } ?? "",
                    state: group.state.map { String(cString: $0) } ?? "",
                    protocolType: group.protocol_type.map { String(cString: $0) } ?? "",
                    members: members,
                    error: group.err == RD_KAFKA_RESP_ERR_NO_ERROR
                        ? nil
                        : String(cString: rd_kafka_err2str(group.err))
                )
            )
        }
        return groups.sorted { $0.id < $1.id }
    }
}

extension Duration {
    /// Whole milliseconds, which is librdkafka's timeout unit.
    var milliseconds: Int {
        let (seconds, attoseconds) = components
        return Int(seconds) * 1000 + Int(attoseconds / 1_000_000_000_000_000)
    }
}
