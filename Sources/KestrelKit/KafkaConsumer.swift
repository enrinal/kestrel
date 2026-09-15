import Crdkafka
import Foundation

/// Owns the consumer `rd_kafka_t` handle and shuts it down exactly once.
private final class ConsumerHandle: @unchecked Sendable {
    let rk: OpaquePointer

    init(_ rk: OpaquePointer) {
        self.rk = rk
    }

    deinit {
        // Leaving the group before destroying avoids a rebalance timeout for
        // the next consumer that joins.
        rd_kafka_consumer_close(rk)
        rd_kafka_destroy(rk)
    }
}

/// Reads records from individual partitions.
///
/// The consumer assigns partitions directly rather than subscribing, so it never
/// joins a rebalance and never commits offsets — browsing a topic must not
/// disturb the consumer groups that actually own it.
///
/// Blocking librdkafka calls run on a private serial queue, so they never stall
/// a cooperative thread.
public actor KafkaConsumer {
    private let handle: ConsumerHandle
    private let diagnostics: KafkaDiagnosticsBox
    private let queue: DispatchQueue

    /// Builds a consumer for `profile`.
    ///
    /// - Throws: ``KafkaError/configuration(key:reason:)`` or
    ///   ``KafkaError/clientCreation(_:)``. No broker is contacted here.
    public init(profile: ClusterProfile, secrets: ClusterSecrets = .none) throws {
        let diagnostics = KafkaDiagnosticsBox()
        let conf = try KafkaConfigurationBuilder.make(
            profile: profile,
            secrets: secrets,
            diagnostics: diagnostics,
            extras: [
                // librdkafka requires a group id even for assign-only use.
                // Auto-commit stays off so browsing never moves anyone's offsets.
                "group.id": "kestrel-browser-\(UUID().uuidString)",
                "enable.auto.commit": "false",
                "enable.auto.offset.store": "false",
                // The EOF event is what lets a read of an empty or short
                // partition finish immediately instead of waiting out a timeout.
                "enable.partition.eof": "true",
                "auto.offset.reset": "error"
            ]
        )

        var errstr = [CChar](repeating: 0, count: 512)
        guard let rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, &errstr, errstr.count) else {
            rd_kafka_conf_destroy(conf)
            throw KafkaError.clientCreation(KafkaConfigurationBuilder.text(errstr))
        }
        rd_kafka_poll_set_consumer(rk)

        self.diagnostics = diagnostics
        self.handle = ConsumerHandle(rk)
        self.queue = DispatchQueue(label: "dev.kestrel.consumer.\(profile.id.uuidString)")
    }

    private func perform<T: Sendable>(
        _ body: @escaping @Sendable (OpaquePointer, KafkaDiagnosticsBox) throws -> T
    ) async throws -> T {
        let handle = self.handle
        let diagnostics = self.diagnostics
        return try await withCheckedThrowingContinuation { continuation in
            queue.async {
                do {
                    continuation.resume(returning: try body(handle.rk, diagnostics))
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    /// Queries the oldest and next-to-be-written offsets for a partition.
    ///
    /// - Returns: `low` is the oldest retained offset; `high` is the offset the
    ///   next produced record will get, so `high - low` is the retained count.
    public func watermarks(
        topic: String,
        partition: Int32,
        timeout: Duration = .seconds(10)
    ) async throws -> (low: Int64, high: Int64) {
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000

        return try await perform { rk, diagnostics in
            var low: Int64 = 0
            var high: Int64 = 0
            let code = rd_kafka_query_watermark_offsets(rk, topic, partition, &low, &high, timeoutMs)
            guard code == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.from(code: code, timeout: timeoutSeconds, detail: diagnostics.take())
            }
            return (low, high)
        }
    }

    /// Reads up to `limit` records from one partition.
    ///
    /// Stops at whichever comes first: `limit` records, the end of the
    /// partition, or `timeout` with nothing further arriving. An empty
    /// partition therefore returns an empty page promptly rather than hanging.
    ///
    /// - Parameters:
    ///   - topic: topic name.
    ///   - partition: partition id.
    ///   - from: where to start; resolved against the current watermarks.
    ///   - limit: maximum number of records to return.
    ///   - timeout: budget for the whole read.
    /// - Returns: the records plus the watermarks they were read against.
    public func fetch(
        topic: String,
        partition: Int32,
        from start: StartPosition = .earliest,
        limit: Int = 100,
        timeout: Duration = .seconds(10)
    ) async throws -> RecordPage {
        let timeoutMs = Int32(timeout.milliseconds)
        let timeoutSeconds = Double(timeout.milliseconds) / 1000

        return try await perform { rk, diagnostics in
            var low: Int64 = 0
            var high: Int64 = 0
            let watermarkCode = rd_kafka_query_watermark_offsets(
                rk, topic, partition, &low, &high, timeoutMs
            )
            guard watermarkCode == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.from(
                    code: watermarkCode,
                    timeout: timeoutSeconds,
                    detail: diagnostics.take()
                )
            }

            let startOffset: Int64 = switch start {
            case .earliest:
                low
            case .latest(let count):
                max(low, high - max(0, count))
            case .offset(let requested):
                min(max(requested, low), high)
            }

            // Nothing retained, or starting past the last record: no read needed.
            guard low < high, startOffset < high else {
                return RecordPage(
                    records: [],
                    startOffset: startOffset,
                    lowWatermark: low,
                    highWatermark: high,
                    reachedEnd: true
                )
            }

            guard let assignment = rd_kafka_topic_partition_list_new(1) else {
                throw KafkaError.clientCreation("rd_kafka_topic_partition_list_new returned null")
            }
            defer { rd_kafka_topic_partition_list_destroy(assignment) }

            guard let entry = rd_kafka_topic_partition_list_add(assignment, topic, partition) else {
                throw KafkaError.configuration(key: topic, reason: "could not assign partition")
            }
            entry.pointee.offset = startOffset

            let assignCode = rd_kafka_assign(rk, assignment)
            guard assignCode == RD_KAFKA_RESP_ERR_NO_ERROR else {
                throw KafkaError.from(
                    code: assignCode,
                    timeout: timeoutSeconds,
                    detail: diagnostics.take()
                )
            }
            defer { rd_kafka_assign(rk, nil) }

            var records: [KafkaRecord] = []
            var reachedEnd = false
            let deadline = Date().addingTimeInterval(timeoutSeconds)

            while records.count < limit, Date() < deadline {
                let remaining = Int32(max(50, Date().distance(to: deadline) * 1000))
                guard let message = rd_kafka_consumer_poll(rk, min(remaining, 1000)) else {
                    continue
                }
                defer { rd_kafka_message_destroy(message) }

                let error = message.pointee.err
                if error == RD_KAFKA_RESP_ERR__PARTITION_EOF {
                    reachedEnd = true
                    break
                }
                if error != RD_KAFKA_RESP_ERR_NO_ERROR {
                    throw KafkaError.from(
                        code: error,
                        timeout: timeoutSeconds,
                        detail: diagnostics.take()
                    )
                }

                records.append(Self.convert(message))
                if let last = records.last, last.offset >= high - 1 {
                    reachedEnd = true
                    break
                }
            }

            return RecordPage(
                records: records,
                startOffset: startOffset,
                lowWatermark: low,
                highWatermark: high,
                reachedEnd: reachedEnd || records.count < limit
            )
        }
    }

    /// Converts a polled message.
    ///
    /// Takes the pointer librdkafka handed back, never a copy of the struct:
    /// `rd_kafka_message_timestamp` and `rd_kafka_message_headers` read fields
    /// that live outside the public `rd_kafka_message_t`, so a copy yields
    /// garbage (a 1970 timestamp and no headers).
    private static func convert(_ message: UnsafeMutablePointer<rd_kafka_message_t>) -> KafkaRecord {
        var kind = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE
        var timestamp: Date?
        let millis = rd_kafka_message_timestamp(message, &kind)
        if millis > 0, kind != RD_KAFKA_TIMESTAMP_NOT_AVAILABLE {
            timestamp = Date(timeIntervalSince1970: Double(millis) / 1000)
        }

        var headers: [RecordHeader] = []
        var rawHeaders: OpaquePointer?
        if rd_kafka_message_headers(message, &rawHeaders) == RD_KAFKA_RESP_ERR_NO_ERROR,
           let rawHeaders {
            var index = 0
            while true {
                var namePointer: UnsafePointer<CChar>?
                var valuePointer: UnsafeRawPointer?
                var size = 0
                let code = rd_kafka_header_get_all(rawHeaders, index, &namePointer, &valuePointer, &size)
                guard code == RD_KAFKA_RESP_ERR_NO_ERROR, let namePointer else { break }
                headers.append(
                    RecordHeader(
                        name: String(cString: namePointer),
                        value: valuePointer.map { Data(bytes: $0, count: size) }
                    )
                )
                index += 1
            }
        }

        let timestampKind: RecordTimestampKind = switch kind {
        case RD_KAFKA_TIMESTAMP_CREATE_TIME: .createTime
        case RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME: .logAppendTime
        default: .unavailable
        }

        return KafkaRecord(
            partition: message.pointee.partition,
            offset: message.pointee.offset,
            timestamp: timestamp,
            timestampKind: timestampKind,
            key: message.pointee.key.map { Data(bytes: $0, count: message.pointee.key_len) },
            value: message.pointee.payload.map { Data(bytes: $0, count: message.pointee.len) },
            headers: headers
        )
    }
}
