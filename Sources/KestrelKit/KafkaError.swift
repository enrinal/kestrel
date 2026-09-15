import Crdkafka
import Foundation

/// Failures surfaced by ``KafkaClient``.
///
/// librdkafka reports everything as a numeric `rd_kafka_resp_err_t`, so the
/// transport-level codes are folded into ``unreachable`` and ``timedOut``, which
/// are the two a user can act on.
public enum KafkaError: Error, LocalizedError, Sendable, Equatable {
    /// A `rd_kafka_conf_set` key was rejected.
    case configuration(key: String, reason: String)
    /// `rd_kafka_new` refused to build a client.
    case clientCreation(String)
    /// No broker in the bootstrap list could be reached. `detail` carries
    /// librdkafka's own text, which is where "Connection refused" comes from.
    case unreachable(detail: String)
    /// The broker did not answer within the requested timeout.
    case timedOut(seconds: Double)
    /// Any other broker-reported error.
    case broker(code: Int32, name: String, detail: String)

    public var errorDescription: String? {
        switch self {
        case .configuration(let key, let reason):
            return "Kafka rejected the setting \"\(key)\": \(reason)"
        case .clientCreation(let reason):
            return "Could not create the Kafka client: \(reason)"
        case .unreachable(let detail):
            return detail.isEmpty
                ? "No broker could be reached. Check the bootstrap servers and that the cluster is running."
                : detail
        case .timedOut(let seconds):
            return "The cluster did not respond within \(Int(seconds))s."
        case .broker(let code, let name, let detail):
            // librdkafka often repeats itself across the code and the callback.
            if detail.isEmpty { return "\(name) (\(code))" }
            if detail == name || detail.contains(name) { return detail }
            return "\(name): \(detail)"
        }
    }

    /// Wraps a librdkafka response code, choosing the most useful case.
    ///
    /// - Parameters:
    ///   - code: the `rd_kafka_resp_err_t` returned by the failing call.
    ///   - timeout: the timeout that was requested, used by ``timedOut``.
    ///   - detail: text collected by the client's error callback, if any.
    static func from(code: rd_kafka_resp_err_t, timeout: Double, detail: String?) -> KafkaError {
        let name = String(cString: rd_kafka_err2str(code))
        switch code {
        case RD_KAFKA_RESP_ERR__TRANSPORT,
             RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
             RD_KAFKA_RESP_ERR__RESOLVE,
             RD_KAFKA_RESP_ERR__AUTHENTICATION:
            return .unreachable(detail: detail ?? name)
        case RD_KAFKA_RESP_ERR__TIMED_OUT,
             RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE:
            // A refused connection also expires the metadata timeout, so prefer
            // the callback detail when the transport already explained itself.
            if let detail, !detail.isEmpty { return .unreachable(detail: detail) }
            return .timedOut(seconds: timeout)
        default:
            return .broker(code: code.rawValue, name: name, detail: detail ?? "")
        }
    }
}
