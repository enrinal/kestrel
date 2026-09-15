import Foundation
import Security

/// The secrets a cluster profile can own.
public enum ClusterSecret: String, CaseIterable, Sendable {
    /// Password for SASL mechanisms that take one (PLAIN, SCRAM).
    case saslPassword
    /// Passphrase protecting the TLS private key PEM.
    case tlsKeyPassphrase
    /// Password for Schema Registry basic auth.
    case schemaRegistryPassword
    /// Password for Kafka Connect basic auth.
    case connectPassword
}

public enum KeychainError: Error, LocalizedError, Equatable {
    case unexpectedStatus(OSStatus)
    case malformedData

    public var errorDescription: String? {
        switch self {
        case .unexpectedStatus(let status):
            let message = SecCopyErrorMessageString(status, nil) as String? ?? "unknown error"
            return "Keychain error \(status): \(message)"
        case .malformedData:
            return "Keychain returned a value that is not UTF-8 text."
        }
    }
}

/// Stores cluster secrets as generic passwords in the login keychain.
///
/// Items are keyed by `<cluster uuid>.<secret>` inside ``service``, so deleting a
/// profile can remove every secret it owns without touching other profiles.
///
/// The keychain ACL trusts the process that created an item. A secret written by
/// the app and read back by a different binary (a test, the CLI) prompts the user
/// for permission, so each process should write the secrets it later reads.
public struct KeychainStore: Sendable {
    public static let defaultService = "dev.kestrel.Kestrel"

    public let service: String

    public init(service: String = KeychainStore.defaultService) {
        self.service = service
    }

    private func account(_ cluster: UUID, _ secret: ClusterSecret) -> String {
        "\(cluster.uuidString).\(secret.rawValue)"
    }

    private func query(_ cluster: UUID, _ secret: ClusterSecret) -> [String: Any] {
        [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account(cluster, secret)
        ]
    }

    /// Writes, replaces, or removes a secret.
    ///
    /// - Parameter value: the secret text. `nil` or empty deletes the item, so
    ///   clearing a password field in the UI clears the Keychain entry too.
    public func set(_ value: String?, secret: ClusterSecret, cluster: UUID) throws {
        guard let value, !value.isEmpty else {
            try remove(secret: secret, cluster: cluster)
            return
        }

        let data = Data(value.utf8)
        let status = SecItemUpdate(
            query(cluster, secret) as CFDictionary,
            [kSecValueData as String: data] as CFDictionary
        )

        switch status {
        case errSecSuccess:
            return
        case errSecItemNotFound:
            var attributes = query(cluster, secret)
            attributes[kSecValueData as String] = data
            attributes[kSecAttrLabel as String] = "Kestrel cluster secret"
            let addStatus = SecItemAdd(attributes as CFDictionary, nil)
            guard addStatus == errSecSuccess else { throw KeychainError.unexpectedStatus(addStatus) }
        default:
            throw KeychainError.unexpectedStatus(status)
        }
    }

    /// Reads a secret, or returns `nil` when none is stored.
    public func get(secret: ClusterSecret, cluster: UUID) throws -> String? {
        var request = query(cluster, secret)
        request[kSecReturnData as String] = true
        request[kSecMatchLimit as String] = kSecMatchLimitOne

        var item: CFTypeRef?
        let status = SecItemCopyMatching(request as CFDictionary, &item)

        switch status {
        case errSecSuccess:
            guard let data = item as? Data else { throw KeychainError.malformedData }
            guard let text = String(data: data, encoding: .utf8) else { throw KeychainError.malformedData }
            return text
        case errSecItemNotFound:
            return nil
        default:
            throw KeychainError.unexpectedStatus(status)
        }
    }

    /// Deletes one secret. Missing items are not an error.
    public func remove(secret: ClusterSecret, cluster: UUID) throws {
        let status = SecItemDelete(query(cluster, secret) as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else {
            throw KeychainError.unexpectedStatus(status)
        }
    }

    /// Deletes every secret owned by a cluster profile.
    public func removeAll(cluster: UUID) throws {
        for secret in ClusterSecret.allCases {
            try remove(secret: secret, cluster: cluster)
        }
    }
}
