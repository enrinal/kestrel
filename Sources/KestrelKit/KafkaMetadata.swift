import Foundation

/// A broker as reported by cluster metadata.
public struct BrokerInfo: Identifiable, Hashable, Sendable {
    public let id: Int32
    public let host: String
    public let port: Int32

    public var endpoint: String { "\(host):\(port)" }

    public init(id: Int32, host: String, port: Int32) {
        self.id = id
        self.host = host
        self.port = port
    }
}

/// One partition of a topic.
public struct PartitionInfo: Identifiable, Hashable, Sendable {
    public let id: Int32
    public let leader: Int32
    public let replicas: [Int32]
    public let inSyncReplicas: [Int32]

    public init(id: Int32, leader: Int32, replicas: [Int32], inSyncReplicas: [Int32]) {
        self.id = id
        self.leader = leader
        self.replicas = replicas
        self.inSyncReplicas = inSyncReplicas
    }
}

/// A topic as reported by cluster metadata.
public struct TopicInfo: Identifiable, Hashable, Sendable {
    public let name: String
    public let partitions: [PartitionInfo]
    /// Non-nil when the broker reported an error for this topic.
    public let error: String?

    public var id: String { name }
    public var partitionCount: Int { partitions.count }
    /// Internal Kafka bookkeeping topics, which are usually hidden in the UI.
    public var isInternal: Bool { name.hasPrefix("__") }

    public init(name: String, partitions: [PartitionInfo], error: String? = nil) {
        self.name = name
        self.partitions = partitions
        self.error = error
    }
}

/// A snapshot of cluster metadata.
public struct ClusterMetadata: Sendable {
    public let brokers: [BrokerInfo]
    public let topics: [TopicInfo]
    /// Broker that answered the metadata request.
    public let originatingBroker: String

    public init(brokers: [BrokerInfo], topics: [TopicInfo], originatingBroker: String) {
        self.brokers = brokers
        self.topics = topics
        self.originatingBroker = originatingBroker
    }
}

/// A member of a consumer group.
public struct ConsumerGroupMember: Identifiable, Hashable, Sendable {
    public let id: String
    public let clientId: String
    public let clientHost: String
    /// Partitions this member currently owns, decoded from its assignment.
    /// Empty while a rebalance is in progress.
    public let assignments: [MemberAssignment]

    /// Topics this member owns partitions of.
    public var topics: [String] { assignments.map(\.topic) }

    /// Partition count across every assigned topic.
    public var partitionCount: Int {
        assignments.reduce(0) { $0 + $1.partitions.count }
    }

    public init(
        id: String,
        clientId: String,
        clientHost: String,
        assignments: [MemberAssignment] = []
    ) {
        self.id = id
        self.clientId = clientId
        self.clientHost = clientHost
        self.assignments = assignments
    }
}

/// A consumer group as reported by the group coordinator.
public struct ConsumerGroupInfo: Identifiable, Hashable, Sendable {
    public let id: String
    public let state: String
    public let protocolType: String
    public let members: [ConsumerGroupMember]
    public let error: String?

    /// Every topic the group's members are assigned, sorted and deduplicated.
    public var subscribedTopics: [String] {
        Set(members.flatMap(\.topics)).sorted()
    }

    public init(
        id: String,
        state: String,
        protocolType: String,
        members: [ConsumerGroupMember],
        error: String? = nil
    ) {
        self.id = id
        self.state = state
        self.protocolType = protocolType
        self.members = members
        self.error = error
    }
}

/// Outcome of a "Test Connection" attempt.
public enum ConnectionTestResult: Sendable {
    case success(brokers: [BrokerInfo], topicCount: Int)
    case failure(String)

    public var isSuccess: Bool {
        if case .success = self { return true }
        return false
    }

    /// One-line summary suitable for a status row.
    public var summary: String {
        switch self {
        case .success(let brokers, let topicCount):
            let list = brokers.map(\.endpoint).joined(separator: ", ")
            let plural = brokers.count == 1 ? "broker" : "brokers"
            return "Connected to \(brokers.count) \(plural) (\(list)) · \(topicCount) topics"
        case .failure(let message):
            return message
        }
    }
}
