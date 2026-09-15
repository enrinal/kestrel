import Foundation

/// State of a connector or one of its tasks, as Connect reports it.
///
/// Unknown states are kept rather than dropped: Connect has added states over
/// the years (`STOPPED` in 3.5), and showing an unfamiliar one is better than
/// pretending a connector has no state at all.
public enum ConnectorState: Hashable, Sendable {
    case running
    case paused
    case stopped
    case failed
    case unassigned
    case restarting
    case other(String)

    public init(_ raw: String) {
        switch raw.uppercased() {
        case "RUNNING": self = .running
        case "PAUSED": self = .paused
        case "STOPPED": self = .stopped
        case "FAILED": self = .failed
        case "UNASSIGNED": self = .unassigned
        case "RESTARTING": self = .restarting
        default: self = .other(raw)
        }
    }

    /// The name Connect uses, for display.
    public var label: String {
        switch self {
        case .running: "RUNNING"
        case .paused: "PAUSED"
        case .stopped: "STOPPED"
        case .failed: "FAILED"
        case .unassigned: "UNASSIGNED"
        case .restarting: "RESTARTING"
        case .other(let raw): raw
        }
    }

    /// True for states that need someone's attention.
    public var isProblem: Bool {
        self == .failed || self == .unassigned
    }
}

/// Whether a connector reads from Kafka or writes to it.
public enum ConnectorKind: String, Hashable, Sendable {
    case source
    case sink
    case unknown

    public init(_ raw: String?) {
        switch raw?.lowercased() {
        case "source": self = .source
        case "sink": self = .sink
        default: self = .unknown
        }
    }
}

/// One task of a connector.
public struct ConnectorTask: Identifiable, Hashable, Sendable {
    public let id: Int
    public let state: ConnectorState
    public let workerID: String
    /// The stack trace Connect attaches to a failed task, if any. This is
    /// usually the only explanation of why a connector stopped working.
    public let trace: String?

    public init(id: Int, state: ConnectorState, workerID: String, trace: String? = nil) {
        self.id = id
        self.state = state
        self.workerID = workerID
        self.trace = trace
    }
}

/// A connector, its status and its configuration.
public struct ConnectorInfo: Identifiable, Hashable, Sendable {
    public var id: String { name }
    public let name: String
    public let kind: ConnectorKind
    public let state: ConnectorState
    public let workerID: String
    public let tasks: [ConnectorTask]
    /// The connector's configuration, as Connect stores it.
    public let config: [String: String]
    /// Trace for a failed connector, as opposed to a failed task.
    public let trace: String?

    public init(
        name: String,
        kind: ConnectorKind,
        state: ConnectorState,
        workerID: String,
        tasks: [ConnectorTask],
        config: [String: String],
        trace: String? = nil
    ) {
        self.name = name
        self.kind = kind
        self.state = state
        self.workerID = workerID
        self.tasks = tasks
        self.config = config
        self.trace = trace
    }

    /// The connector class, which is the one config key worth showing first.
    public var connectorClass: String? {
        config["connector.class"]
    }

    /// Tasks that are not running, which is what a status column should lead on.
    public var failedTasks: [ConnectorTask] {
        tasks.filter(\.state.isProblem)
    }

    /// True when the connector or any of its tasks needs attention.
    public var needsAttention: Bool {
        state.isProblem || !failedTasks.isEmpty
    }
}

/// Identity of a Connect worker, for the cluster editor's test button.
public struct ConnectWorker: Sendable, Equatable {
    public let version: String
    public let kafkaClusterID: String?
    public let commit: String?

    public init(version: String, kafkaClusterID: String? = nil, commit: String? = nil) {
        self.version = version
        self.kafkaClusterID = kafkaClusterID
        self.commit = commit
    }
}

/// Why a Connect request failed.
///
/// Every case says what to do about it: the acceptance line for this slice is
/// that an unreachable Connect is explained rather than shown as an empty list.
public enum KafkaConnectError: LocalizedError, Equatable {
    case notConfigured
    case badURL(String)
    case unreachable(String)
    case unauthorized
    case notFound(String)
    case rebalancing
    case http(status: Int, message: String)
    case malformedResponse(String)

    public var errorDescription: String? {
        switch self {
        case .notConfigured:
            "No Kafka Connect URL is set for this cluster. Add one in Edit Cluster to list connectors."
        case .badURL(let url):
            "Kafka Connect URL is not usable: \(url)"
        case .unreachable(let detail):
            "Kafka Connect is unreachable: \(detail)"
        case .unauthorized:
            "Kafka Connect rejected the credentials for this cluster."
        case .notFound(let what):
            "Kafka Connect has no \(what)."
        case .rebalancing:
            "The Connect cluster is rebalancing. Try again in a moment."
        case .http(let status, let message):
            "Kafka Connect returned \(status): \(message)"
        case .malformedResponse(let detail):
            "Kafka Connect sent something unexpected: \(detail)"
        }
    }
}

/// Talks to a Kafka Connect worker's REST API.
///
/// An actor because it owns a `URLSession`, and because the pause and resume
/// calls are asynchronous at the far end: Connect returns 202 and rebalances,
/// so a caller that fires several at once would otherwise race its own reads.
public actor KafkaConnectClient {
    private let baseURL: URL
    private let authorization: String?
    private let session: URLSession

    /// - Parameters:
    ///   - url: worker base URL, such as `http://localhost:18083`.
    ///   - user: username for basic auth, when the worker is behind one.
    ///   - password: password for basic auth.
    ///   - timeout: how long to wait before calling the worker unreachable.
    public init(
        url: String,
        user: String? = nil,
        password: String? = nil,
        timeout: TimeInterval = 10
    ) throws {
        guard let parsed = URL(string: url), parsed.scheme != nil, parsed.host != nil else {
            throw KafkaConnectError.badURL(url)
        }
        self.baseURL = parsed

        if let user, !user.isEmpty {
            let pair = Data("\(user):\(password ?? "")".utf8).base64EncodedString()
            self.authorization = "Basic \(pair)"
        } else {
            self.authorization = nil
        }

        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = timeout
        self.session = URLSession(configuration: configuration)
    }

    /// Lists every connector with its status and configuration.
    ///
    /// One request: `?expand=status&expand=info` gets the lot, where the
    /// obvious loop over `/connectors/{name}/status` would be a request per
    /// connector and would tear if a connector vanished mid-list.
    public func connectors() async throws -> [ConnectorInfo] {
        let body = try await request("/connectors?expand=status&expand=info", describing: "connectors")

        guard let object = try? JSONSerialization.jsonObject(with: body) else {
            throw KafkaConnectError.malformedResponse("the connector list was not JSON")
        }

        // A worker older than 2.3 ignores `expand` and returns a bare array of
        // names, so fall back to fetching each one.
        if let names = object as? [String] {
            var result: [ConnectorInfo] = []
            for name in names.sorted() {
                result.append(try await connector(named: name))
            }
            return result
        }

        guard let expanded = object as? [String: Any] else {
            throw KafkaConnectError.malformedResponse("the connector list was neither a list nor a map")
        }

        return expanded.keys.sorted().compactMap { name in
            guard let entry = expanded[name] as? [String: Any] else { return nil }
            return Self.parse(
                name: name,
                info: entry["info"] as? [String: Any],
                status: entry["status"] as? [String: Any]
            )
        }
    }

    /// Fetches one connector's status and configuration.
    public func connector(named name: String) async throws -> ConnectorInfo {
        let path = Self.escape(name)
        let info = try await json(at: "/connectors/\(path)", describing: "connector \(name)")
        let status = try await json(at: "/connectors/\(path)/status", describing: "connector \(name)")

        guard let parsed = Self.parse(name: name, info: info, status: status) else {
            throw KafkaConnectError.malformedResponse("connector \(name) could not be read")
        }
        return parsed
    }

    /// Pauses a connector and its tasks. Connect accepts this asynchronously,
    /// so the state does not change until the worker has rebalanced.
    public func pause(_ name: String) async throws {
        _ = try await request(
            "/connectors/\(Self.escape(name))/pause",
            method: "PUT",
            describing: "connector \(name)"
        )
    }

    /// Resumes a paused connector, also asynchronously.
    public func resume(_ name: String) async throws {
        _ = try await request(
            "/connectors/\(Self.escape(name))/resume",
            method: "PUT",
            describing: "connector \(name)"
        )
    }

    /// Restarts a connector.
    ///
    /// - Parameters:
    ///   - name: connector to restart.
    ///   - includeTasks: restart the tasks too, not just the connector
    ///     instance. Restarting the instance alone rarely fixes anything,
    ///     because a failure is usually in a task.
    ///   - onlyFailed: restart only what has failed, leaving running tasks
    ///     alone.
    public func restart(
        _ name: String,
        includeTasks: Bool = true,
        onlyFailed: Bool = false
    ) async throws {
        let query = "?includeTasks=\(includeTasks)&onlyFailed=\(onlyFailed)"
        _ = try await request(
            "/connectors/\(Self.escape(name))/restart\(query)",
            method: "POST",
            describing: "connector \(name)"
        )
    }

    /// Restarts one task of a connector.
    public func restartTask(_ task: Int, of name: String) async throws {
        _ = try await request(
            "/connectors/\(Self.escape(name))/tasks/\(task)/restart",
            method: "POST",
            describing: "task \(task) of connector \(name)"
        )
    }

    /// Reads the worker's identity, for the cluster editor's test button.
    public func worker() async throws -> ConnectWorker {
        let root = try await json(at: "/", describing: "worker information")
        guard let version = root?["version"] as? String else {
            throw KafkaConnectError.malformedResponse("the worker reported no version")
        }
        return ConnectWorker(
            version: version,
            kafkaClusterID: root?["kafka_cluster_id"] as? String,
            commit: root?["commit"] as? String
        )
    }

    /// Checks the worker answers, and summarises what was found.
    public func test() async throws -> String {
        let worker = try await worker()
        let count = try await connectors().count
        return "Connect \(worker.version) · \(count) connector\(count == 1 ? "" : "s")"
    }

    // MARK: Requests

    private func json(at path: String, describing what: String) async throws -> [String: Any]? {
        let body = try await request(path, describing: what)
        guard !body.isEmpty else { return nil }
        guard let object = try? JSONSerialization.jsonObject(with: body) as? [String: Any] else {
            throw KafkaConnectError.malformedResponse("\(what) was not a JSON object")
        }
        return object
    }

    private func request(
        _ path: String,
        method: String = "GET",
        describing what: String
    ) async throws -> Data {
        guard let url = URL(string: path, relativeTo: baseURL) else {
            throw KafkaConnectError.badURL(baseURL.absoluteString + path)
        }

        var request = URLRequest(url: url)
        request.httpMethod = method
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        if let authorization {
            request.setValue(authorization, forHTTPHeaderField: "Authorization")
        }

        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            throw KafkaConnectError.unreachable(error.localizedDescription)
        }

        guard let http = response as? HTTPURLResponse else {
            throw KafkaConnectError.malformedResponse("not an HTTP reply")
        }

        switch http.statusCode {
        case 200..<300:
            return data
        case 401, 403:
            throw KafkaConnectError.unauthorized
        case 404:
            throw KafkaConnectError.notFound(what)
        case 409:
            // Connect uses 409 for "rebalance in progress", which is transient
            // and worth saying so rather than reporting as a generic failure.
            throw KafkaConnectError.rebalancing
        default:
            let message = (try? JSONSerialization.jsonObject(with: data) as? [String: Any])
                .flatMap { $0?["message"] as? String }
                ?? String(data: data, encoding: .utf8)
                ?? "no detail"
            throw KafkaConnectError.http(status: http.statusCode, message: message)
        }
    }

    /// Percent-encodes a connector name for a URL path.
    ///
    /// Connector names may contain spaces and slashes, which would otherwise
    /// change the path being requested.
    private static func escape(_ name: String) -> String {
        name.addingPercentEncoding(withAllowedCharacters: .alphanumerics.union(.init(charactersIn: "-._~")))
            ?? name
    }

    /// Builds a connector from the `info` and `status` halves of a reply.
    ///
    /// Either half may be missing — a connector added a moment ago has config
    /// but no status yet — so both are optional and the gaps are filled with
    /// what can be said truthfully.
    private static func parse(
        name: String,
        info: [String: Any]?,
        status: [String: Any]?
    ) -> ConnectorInfo? {
        let config = (info?["config"] as? [String: Any])?.compactMapValues { $0 as? String } ?? [:]
        let connector = status?["connector"] as? [String: Any]
        let rawTasks = status?["tasks"] as? [[String: Any]] ?? []

        let tasks = rawTasks.compactMap { task -> ConnectorTask? in
            guard let id = task["id"] as? Int else { return nil }
            return ConnectorTask(
                id: id,
                state: ConnectorState(task["state"] as? String ?? ""),
                workerID: task["worker_id"] as? String ?? "",
                trace: task["trace"] as? String
            )
        }

        return ConnectorInfo(
            name: name,
            kind: ConnectorKind(status?["type"] as? String ?? info?["type"] as? String),
            state: ConnectorState(connector?["state"] as? String ?? ""),
            workerID: connector?["worker_id"] as? String ?? "",
            tasks: tasks.sorted { $0.id < $1.id },
            config: config,
            trace: connector?["trace"] as? String
        )
    }
}
