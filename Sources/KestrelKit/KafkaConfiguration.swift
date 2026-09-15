import Crdkafka
import Foundation

/// Collects the diagnostics librdkafka reports through its callbacks.
///
/// Both callbacks run on librdkafka's own threads, so the box is lock guarded.
///
/// Two sources are kept because they say different things. The error callback
/// gives the summary ("1/1 brokers are down") while the log callback gives the
/// cause ("Connection refused"), and only the pair is actually useful.
final class KafkaDiagnosticsBox: @unchecked Sendable {
    private let lock = NSLock()
    private var summary: String?
    private var cause: String?

    /// Records text from librdkafka's `error_cb`.
    func record(summary text: String) {
        lock.lock()
        summary = text
        lock.unlock()
    }

    /// Records a failure line from librdkafka's `log_cb`.
    func record(cause text: String) {
        lock.lock()
        cause = text
        lock.unlock()
    }

    /// Returns the best available explanation and clears what was consumed.
    func take() -> String? {
        lock.lock()
        defer {
            summary = nil
            cause = nil
            lock.unlock()
        }

        switch (summary, cause) {
        case (let summary?, let cause?): return "\(summary) — \(cause)"
        case (let summary?, nil): return summary
        case (nil, let cause?): return cause
        case (nil, nil): return nil
        }
    }
}

/// Carries one message's delivery report back from librdkafka's callback.
///
/// A box is retained per produced message and passed as the message opaque, so
/// concurrent produces cannot collide. The callback releases it.
final class DeliveryBox: @unchecked Sendable {
    private let lock = NSLock()
    private var outcome: (code: rd_kafka_resp_err_t, partition: Int32, offset: Int64)?

    func record(code: rd_kafka_resp_err_t, partition: Int32, offset: Int64) {
        lock.lock()
        outcome = (code, partition, offset)
        lock.unlock()
    }

    /// The delivery outcome, or `nil` if the report has not arrived yet.
    var result: (code: rd_kafka_resp_err_t, partition: Int32, offset: Int64)? {
        lock.lock()
        defer { lock.unlock() }
        return outcome
    }
}

/// Turns a ``ClusterProfile`` into an `rd_kafka_conf_t`.
///
/// Shared by ``KafkaClient`` and ``KafkaConsumer`` so both speak to a cluster
/// with identical security settings.
enum KafkaConfigurationBuilder {
    /// Builds a configuration, applying only the fields the profile's security
    /// protocol actually uses.
    ///
    /// - Parameters:
    ///   - profile: the cluster to connect to.
    ///   - secrets: password and passphrase, when the protocol needs them.
    ///   - diagnostics: box wired to the error and log callbacks. The caller
    ///     must keep it alive for as long as the client lives.
    ///   - extras: additional librdkafka keys, applied last so they win.
    /// - Returns: a configuration the caller owns. `rd_kafka_new` takes
    ///   ownership only on success; on failure the config is already destroyed.
    static func make(
        profile: ClusterProfile,
        secrets: ClusterSecrets,
        diagnostics: KafkaDiagnosticsBox,
        extras: [String: String] = [:]
    ) throws -> OpaquePointer {
        guard let conf = rd_kafka_conf_new() else {
            throw KafkaError.clientCreation("rd_kafka_conf_new returned null")
        }

        func set(_ key: String, _ value: String) throws {
            var errstr = [CChar](repeating: 0, count: 512)
            guard rd_kafka_conf_set(conf, key, value, &errstr, errstr.count) == RD_KAFKA_CONF_OK else {
                let reason = text(errstr)
                rd_kafka_conf_destroy(conf)
                throw KafkaError.configuration(key: key, reason: reason)
            }
        }

        try set("bootstrap.servers", profile.bootstrapServers)
        try set("client.id", "kestrel")
        try set("security.protocol", profile.securityProtocol.rawValue)
        // Keep librdkafka's stderr chatter down; errors arrive via the callback.
        try set("log_level", "3")

        if profile.securityProtocol.usesSASL {
            if let mechanism = profile.saslMechanism {
                try set("sasl.mechanism", mechanism.rawValue)
            }
            if !profile.saslUsername.isEmpty {
                try set("sasl.username", profile.saslUsername)
            }
            if let password = secrets.saslPassword, !password.isEmpty {
                try set("sasl.password", password)
            }
        }

        if profile.securityProtocol.usesTLS {
            if !profile.tls.caLocation.isEmpty {
                try set("ssl.ca.location", profile.tls.caLocation)
            }
            if !profile.tls.certificateLocation.isEmpty {
                try set("ssl.certificate.location", profile.tls.certificateLocation)
            }
            if !profile.tls.keyLocation.isEmpty {
                try set("ssl.key.location", profile.tls.keyLocation)
            }
            if let passphrase = secrets.tlsKeyPassphrase, !passphrase.isEmpty {
                try set("ssl.key.password", passphrase)
            }
            try set(
                "ssl.endpoint.identification.algorithm",
                profile.tls.verifyHostname ? "https" : "none"
            )
        }

        for (key, value) in extras {
            try set(key, value)
        }

        // The box outlives the config because the client holds it strongly.
        rd_kafka_conf_set_opaque(conf, Unmanaged.passUnretained(diagnostics).toOpaque())

        rd_kafka_conf_set_error_cb(conf) { _, _, reason, opaque in
            guard let opaque, let reason else { return }
            let box = Unmanaged<KafkaDiagnosticsBox>.fromOpaque(opaque).takeUnretainedValue()
            box.record(summary: String(cString: reason))
        }

        // Installing a log callback also stops librdkafka writing to stderr,
        // which otherwise floods the console once a broker is unreachable.
        rd_kafka_conf_set_log_cb(conf) { rk, level, facility, message in
            guard let rk, let message, level <= 3 else { return }
            guard let opaque = rd_kafka_opaque(rk) else { return }
            let facilityName = facility.map { String(cString: $0) } ?? ""
            guard facilityName == "FAIL" || facilityName == "ERROR" else { return }
            let box = Unmanaged<KafkaDiagnosticsBox>.fromOpaque(opaque).takeUnretainedValue()
            // Fully qualified: an unqualified call would capture the metatype,
            // and a C function pointer cannot capture context.
            box.record(cause: KafkaConfigurationBuilder.trimLogNoise(String(cString: message)))
        }

        // Delivery reports are how a produce learns its assigned offset. The
        // per-message opaque is a retained DeliveryBox; release it here.
        rd_kafka_conf_set_dr_msg_cb(conf) { _, message, _ in
            guard let message, let opaque = message.pointee._private else { return }
            let box = Unmanaged<DeliveryBox>.fromOpaque(opaque).takeRetainedValue()
            box.record(
                code: message.pointee.err,
                partition: message.pointee.partition,
                offset: message.pointee.offset
            )
        }

        return conf
    }

    /// Decodes a librdkafka `char[]` error buffer up to its null terminator.
    static func text(_ buffer: [CChar]) -> String {
        String(decoding: buffer.prefix { $0 != 0 }.map { UInt8(bitPattern: $0) }, as: UTF8.self)
    }

    /// Strips librdkafka's log decoration: the leading `[thrd:…]: ` tag and the
    /// trailing timing note such as `(after 0ms in state CONNECT)`.
    static func trimLogNoise(_ message: String) -> String {
        var text = message
        if text.hasPrefix("[thrd:"), let end = text.range(of: "]: ") {
            text = String(text[end.upperBound...])
        }
        if let range = text.range(of: " (after ", options: .backwards) {
            text = String(text[text.startIndex..<range.lowerBound])
        }
        return text
    }
}
