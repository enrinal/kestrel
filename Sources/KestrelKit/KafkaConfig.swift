import Foundation

/// A configuration key/value as reported by DescribeConfigs.
public struct ConfigEntry: Identifiable, Hashable, Sendable {
    public let name: String
    /// The value, or `nil` for sensitive entries — brokers withhold those.
    public let value: String?
    /// True when the broker is serving its own default rather than an override.
    public let isDefault: Bool
    /// True when the entry cannot be changed with AlterConfigs.
    public let isReadOnly: Bool
    /// True when the broker refuses to disclose the value.
    public let isSensitive: Bool

    public var id: String { name }

    /// Value for display: sensitive entries render as a placeholder.
    public var displayValue: String {
        if isSensitive { return "••••••" }
        guard let value, !value.isEmpty else { return "—" }
        return value
    }

    public init(
        name: String,
        value: String?,
        isDefault: Bool,
        isReadOnly: Bool,
        isSensitive: Bool
    ) {
        self.name = name
        self.value = value
        self.isDefault = isDefault
        self.isReadOnly = isReadOnly
        self.isSensitive = isSensitive
    }
}

/// Something whose configuration can be described.
public enum ConfigResource: Hashable, Sendable {
    case broker(Int32)
    case topic(String)

    /// Resource name in the form the Kafka protocol expects.
    var name: String {
        switch self {
        case .broker(let id): return String(id)
        case .topic(let name): return name
        }
    }
}
