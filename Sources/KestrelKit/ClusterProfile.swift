import Foundation

/// Kafka `security.protocol` values.
public enum SecurityProtocol: String, Codable, CaseIterable, Sendable, Identifiable {
    case plaintext = "PLAINTEXT"
    case ssl = "SSL"
    case saslPlaintext = "SASL_PLAINTEXT"
    case saslSSL = "SASL_SSL"

    public var id: String { rawValue }

    /// True when the protocol performs a SASL handshake and therefore needs a
    /// mechanism and, for most mechanisms, a username and password.
    public var usesSASL: Bool {
        self == .saslPlaintext || self == .saslSSL
    }

    /// True when the connection is TLS wrapped and the TLS settings apply.
    public var usesTLS: Bool {
        self == .ssl || self == .saslSSL
    }
}

/// Kafka `sasl.mechanism` values.
public enum SASLMechanism: String, Codable, CaseIterable, Sendable, Identifiable {
    case plain = "PLAIN"
    case scramSHA256 = "SCRAM-SHA-256"
    case scramSHA512 = "SCRAM-SHA-512"
    case gssapi = "GSSAPI"
    case oauthBearer = "OAUTHBEARER"

    public var id: String { rawValue }
}

/// TLS material locations. Paths only — the key passphrase is a secret and lives
/// in the Keychain.
public struct TLSSettings: Hashable, Codable, Sendable {
    public var caLocation: String
    public var certificateLocation: String
    public var keyLocation: String
    public var verifyHostname: Bool

    public init(
        caLocation: String = "",
        certificateLocation: String = "",
        keyLocation: String = "",
        verifyHostname: Bool = true
    ) {
        self.caLocation = caLocation
        self.certificateLocation = certificateLocation
        self.keyLocation = keyLocation
        self.verifyHostname = verifyHostname
    }
}

/// Where to find a Confluent Schema Registry, for decoding Avro.
///
/// The URL and username only — the password is a secret and lives in the
/// Keychain, like the SASL password.
public struct SchemaRegistrySettings: Hashable, Codable, Sendable {
    /// Base URL, such as `http://localhost:18081`. Empty means the cluster has
    /// no registry, which is what makes an Avro record undecodable.
    public var url: String
    /// Username for basic auth. Empty means the registry needs none.
    public var user: String

    public init(url: String = "", user: String = "") {
        self.url = url
        self.user = user
    }

    /// True when a registry is set up well enough to try.
    public var isConfigured: Bool {
        !url.trimmingCharacters(in: .whitespaces).isEmpty
    }
}

/// Where to find a Kafka Connect worker, for listing connectors.
///
/// Same shape as ``SchemaRegistrySettings``, and for the same reason: the URL
/// and username belong in the profile, the password in the Keychain.
public struct ConnectSettings: Hashable, Codable, Sendable {
    /// Worker base URL, such as `http://localhost:8083`. Empty means the
    /// cluster has no Connect, and the sidebar shows no Connect branch.
    public var url: String
    /// Username for basic auth. Empty means the worker needs none.
    public var user: String

    public init(url: String = "", user: String = "") {
        self.url = url
        self.user = user
    }

    /// True when a worker is set up well enough to try.
    public var isConfigured: Bool {
        !url.trimmingCharacters(in: .whitespaces).isEmpty
    }
}

/// A saved Kafka cluster connection.
///
/// The type deliberately has no password or passphrase fields, so encoding a
/// profile cannot leak a secret to disk. Secrets are held in the Keychain under
/// the profile's `id`; see ``KeychainStore`` and ``ClusterSecret``.
public struct ClusterProfile: Identifiable, Hashable, Codable, Sendable {
    public var id: UUID
    public var name: String
    public var bootstrapServers: String
    public var securityProtocol: SecurityProtocol
    public var saslMechanism: SASLMechanism?
    public var saslUsername: String
    public var tls: TLSSettings
    public var schemaRegistry: SchemaRegistrySettings
    public var connect: ConnectSettings

    public init(
        id: UUID = UUID(),
        name: String,
        bootstrapServers: String,
        securityProtocol: SecurityProtocol = .plaintext,
        saslMechanism: SASLMechanism? = nil,
        saslUsername: String = "",
        tls: TLSSettings = TLSSettings(),
        schemaRegistry: SchemaRegistrySettings = SchemaRegistrySettings(),
        connect: ConnectSettings = ConnectSettings()
    ) {
        self.id = id
        self.name = name
        self.bootstrapServers = bootstrapServers
        self.securityProtocol = securityProtocol
        self.saslMechanism = saslMechanism
        self.saslUsername = saslUsername
        self.tls = tls
        self.schemaRegistry = schemaRegistry
        self.connect = connect
    }

    /// Decoded field by field so a profile saved before a field existed still
    /// loads. The synthesised decoder would reject the older file outright,
    /// losing every cluster someone had saved.
    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(UUID.self, forKey: .id)
        name = try container.decode(String.self, forKey: .name)
        bootstrapServers = try container.decode(String.self, forKey: .bootstrapServers)
        securityProtocol = try container.decodeIfPresent(
            SecurityProtocol.self,
            forKey: .securityProtocol
        ) ?? .plaintext
        saslMechanism = try container.decodeIfPresent(SASLMechanism.self, forKey: .saslMechanism)
        saslUsername = try container.decodeIfPresent(String.self, forKey: .saslUsername) ?? ""
        tls = try container.decodeIfPresent(TLSSettings.self, forKey: .tls) ?? TLSSettings()
        schemaRegistry = try container.decodeIfPresent(
            SchemaRegistrySettings.self,
            forKey: .schemaRegistry
        ) ?? SchemaRegistrySettings()
        connect = try container.decodeIfPresent(ConnectSettings.self, forKey: .connect)
            ?? ConnectSettings()
    }
}
