import Foundation
import KestrelKit

/// Everything a command needs: which cluster, how to talk to it, and where to
/// write the results.
struct Context {
    let profile: ClusterProfile
    let secrets: ClusterSecrets
    let output: Output
    let timeout: Duration

    func client() throws -> KafkaClient {
        try KafkaClient(profile: profile, secrets: secrets)
    }

    func consumer() throws -> KafkaConsumer {
        try KafkaConsumer(profile: profile, secrets: secrets)
    }

    /// The cluster's Schema Registry client.
    ///
    /// - Throws: ``CLIFailure`` when the profile has no registry. The shared
    ///   ``SchemaRegistryError/notConfigured`` is not reused here because its
    ///   sentence tells the reader to open Edit Cluster, which is not advice a
    ///   terminal can act on.
    func registry() throws -> SchemaRegistryClient {
        guard profile.schemaRegistry.isConfigured else {
            throw CLIFailure(
                "cluster \(profile.name) has no Schema Registry. Set one in the app, "
                    + "under Edit Cluster → Schema Registry."
            )
        }
        return try SchemaRegistryClient(
            url: profile.schemaRegistry.url,
            user: profile.schemaRegistry.user,
            password: secrets.schemaRegistryPassword
        )
    }

    /// The cluster's Kafka Connect client.
    func connect() throws -> KafkaConnectClient {
        guard profile.connect.isConfigured else {
            throw CLIFailure(
                "cluster \(profile.name) has no Kafka Connect URL. Set one in the app, "
                    + "under Edit Cluster → Kafka Connect."
            )
        }
        return try KafkaConnectClient(
            url: profile.connect.url,
            user: profile.connect.user,
            password: secrets.connectPassword
        )
    }
}

/// Finds the cluster a command should run against.
enum ClusterResolver {
    /// Resolves `--cluster` against the saved profiles, or `--bootstrap` into a
    /// throwaway profile.
    ///
    /// Two ways in on purpose: `--cluster local` shares the GUI's saved
    /// settings, including TLS and SASL, while `--bootstrap host:9092` gets a
    /// plaintext cluster going with nothing saved at all.
    static func resolve(_ arguments: inout Arguments) throws -> (ClusterProfile, ClusterSecrets) {
        let name = arguments.string("cluster")
        let bootstrap = arguments.string("bootstrap")
        let compression = try compression(&arguments)

        if let name, bootstrap != nil {
            throw UsageError("pass either --cluster or --bootstrap, not both")
        }

        if let bootstrap {
            // No saved profile, so no Keychain and no secrets: a cluster given
            // on the command line is plaintext or nothing.
            return (
                ClusterProfile(
                    name: "(--bootstrap)",
                    bootstrapServers: bootstrap,
                    compression: compression ?? .none
                ),
                .none
            )
        }

        guard let name else {
            throw UsageError("--cluster <name> is required (or --bootstrap <host:port>)")
        }

        let profiles = try ClusterProfileRepository().load()
        guard !profiles.isEmpty else {
            throw UsageError(
                "no saved clusters. Add one in the app, or pass --bootstrap <host:port>."
            )
        }

        guard var profile = profiles.first(where: { $0.name.caseInsensitiveCompare(name) == .orderedSame })
        else {
            let known = profiles.map(\.name).sorted().joined(separator: ", ")
            throw UsageError("no saved cluster named \(name). Known clusters: \(known)")
        }

        // The flag overrides the saved codec for this run only; the profile is
        // never written back, so a one-off gzip produce cannot change what the
        // app does next time it connects.
        if let compression {
            profile.compression = compression
        }

        // Reading a secret the app wrote makes macOS ask the user to allow it,
        // because the Keychain item is trusted by the binary that created it
        // and `kestrel` is a different binary. In a script, or over ssh, that
        // prompt cannot be answered and the command appears to hang, so there
        // has to be a way to say "do not even try".
        if arguments.flag("no-keychain") {
            return (profile, .none)
        }
        return (profile, secrets(for: profile))
    }

    /// Reads `--compression`, or `nil` when it was not given.
    ///
    /// - Throws: ``UsageError`` naming the codecs, because a rejected value
    ///   here is a typo, and the alternative is producing uncompressed while
    ///   reporting success.
    private static func compression(_ arguments: inout Arguments) throws -> CompressionCodec? {
        guard let raw = arguments.string("compression") else { return nil }
        guard let codec = CompressionCodec(rawValue: raw.lowercased()) else {
            let known = CompressionCodec.allCases.map(\.rawValue).joined(separator: ", ")
            throw UsageError("--compression must be one of: \(known), got \(raw)")
        }
        return codec
    }

    /// Reads only the secrets the profile's settings actually call for.
    ///
    /// Each read of a secret the app wrote prompts for Keychain permission,
    /// because the item is trusted by the binary that created it. Skipping the
    /// reads that cannot matter means a plaintext cluster with no registry and
    /// no Connect never prompts at all.
    static func secrets(for profile: ClusterProfile) -> ClusterSecrets {
        let keychain = KeychainStore()
        var secrets = ClusterSecrets()

        func read(_ secret: ClusterSecret) -> String? {
            do {
                return try keychain.get(secret: secret, cluster: profile.id)
            } catch {
                printError("could not read \(secret.rawValue) from the Keychain: \(error.localizedDescription)")
                return nil
            }
        }

        if profile.securityProtocol.usesSASL {
            secrets.saslPassword = read(.saslPassword)
        }
        if profile.securityProtocol.usesTLS {
            secrets.tlsKeyPassphrase = read(.tlsKeyPassphrase)
        }
        if !profile.schemaRegistry.user.isEmpty {
            secrets.schemaRegistryPassword = read(.schemaRegistryPassword)
        }
        if !profile.connect.user.isEmpty {
            secrets.connectPassword = read(.connectPassword)
        }

        return secrets
    }
}

/// Renders a payload for the terminal.
enum PayloadText {
    /// Text when the bytes are text, base64 when they are not.
    ///
    /// Writing raw bytes to a terminal can leave it in a broken state, so
    /// anything that is not valid UTF-8 is base64-encoded and labelled.
    static func describe(_ data: Data?) -> String {
        guard let data else { return "null" }
        if let text = String(data: data, encoding: .utf8) { return text }
        return "base64:\(data.base64EncodedString())"
    }

    /// The same, shortened for a table cell.
    static func preview(_ data: Data?, limit: Int = 60) -> String {
        let text = describe(data).replacingOccurrences(of: "\n", with: " ")
        return text.count > limit ? String(text.prefix(limit - 1)) + "…" : text
    }
}
