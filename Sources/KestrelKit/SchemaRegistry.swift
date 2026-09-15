import Foundation

/// A schema as Schema Registry reports it.
public struct RegisteredSchema: Sendable, Equatable {
    public let id: Int32
    /// The schema document, as JSON text.
    public let text: String
    /// Subject and version, when the schema was fetched by subject.
    public let subject: String?
    public let version: Int?

    public init(id: Int32, text: String, subject: String? = nil, version: Int? = nil) {
        self.id = id
        self.text = text
        self.subject = subject
        self.version = version
    }
}

/// Why a registry request failed.
///
/// Each case says what to do about it, because the alternative in the detail
/// pane is a hex dump and no explanation.
public enum SchemaRegistryError: LocalizedError, Equatable {
    case notConfigured
    case badURL(String)
    case unreachable(String)
    case unauthorized
    case notFound(String)
    case http(status: Int, message: String)
    case malformedResponse(String)

    public var errorDescription: String? {
        switch self {
        case .notConfigured:
            "This record is Avro, but no Schema Registry is set for this cluster. Add one in Edit Cluster to decode it."
        case .badURL(let url):
            "Schema Registry URL is not usable: \(url)"
        case .unreachable(let detail):
            "Schema Registry is unreachable: \(detail)"
        case .unauthorized:
            "Schema Registry rejected the credentials for this cluster."
        case .notFound(let what):
            "Schema Registry has no \(what)."
        case .http(let status, let message):
            "Schema Registry returned \(status): \(message)"
        case .malformedResponse(let detail):
            "Schema Registry sent something unexpected: \(detail)"
        }
    }
}

/// Reads schemas from a Confluent Schema Registry.
///
/// An actor holding a cache: a topic's records nearly all share a handful of
/// schema ids, and refetching one per record would make the message browser
/// crawl. Schemas are immutable once registered, so caching by id is safe.
public actor SchemaRegistryClient {
    private let baseURL: URL
    private let authorization: String?
    private let session: URLSession
    private var schemasByID: [Int32: RegisteredSchema] = [:]
    /// Parsed forms, kept because parsing is the expensive part.
    private var parsedByID: [Int32: (schema: AvroSchema, named: [String: AvroSchema])] = [:]

    /// - Parameters:
    ///   - url: registry base URL, such as `http://localhost:18081`.
    ///   - user: username for basic auth, when the registry needs it.
    ///   - password: password for basic auth.
    ///   - timeout: how long to wait before calling the registry unreachable.
    public init(
        url: String,
        user: String? = nil,
        password: String? = nil,
        timeout: TimeInterval = 10
    ) throws {
        guard let parsed = URL(string: url), parsed.scheme != nil, parsed.host != nil else {
            throw SchemaRegistryError.badURL(url)
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

    /// Fetches a schema by id, from the cache when it has been seen before.
    public func schema(id: Int32) async throws -> RegisteredSchema {
        if let cached = schemasByID[id] { return cached }

        let body = try await get("/schemas/ids/\(id)", describing: "schema \(id)")
        guard let object = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
              let text = object["schema"] as? String
        else {
            throw SchemaRegistryError.malformedResponse("no schema in the reply for id \(id)")
        }

        let schema = RegisteredSchema(id: id, text: text)
        schemasByID[id] = schema
        return schema
    }

    /// Fetches a schema by id and parses it, caching both.
    public func parsedSchema(
        id: Int32
    ) async throws -> (schema: AvroSchema, named: [String: AvroSchema]) {
        if let cached = parsedByID[id] { return cached }

        let registered = try await schema(id: id)
        let parsed = try AvroSchemaParser.parse(registered.text)
        parsedByID[id] = parsed
        return parsed
    }

    /// Lists the subjects the registry knows.
    public func subjects() async throws -> [String] {
        let body = try await get("/subjects", describing: "subjects")
        guard let names = try? JSONSerialization.jsonObject(with: body) as? [String] else {
            throw SchemaRegistryError.malformedResponse("subjects were not a list")
        }
        return names.sorted()
    }

    /// Fetches the newest schema registered under a subject.
    public func latestSchema(subject: String) async throws -> RegisteredSchema {
        let path = "/subjects/\(subject.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? subject)/versions/latest"
        let body = try await get(path, describing: "subject \(subject)")

        guard let object = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
              let text = object["schema"] as? String,
              let id = object["id"] as? Int
        else {
            throw SchemaRegistryError.malformedResponse("no schema in the reply for \(subject)")
        }

        let schema = RegisteredSchema(
            id: Int32(id),
            text: text,
            subject: subject,
            version: object["version"] as? Int
        )
        schemasByID[schema.id] = RegisteredSchema(id: schema.id, text: text)
        return schema
    }

    /// Checks the registry answers at all, for the cluster editor's test button.
    ///
    /// - Returns: a sentence describing what was found.
    public func test() async throws -> String {
        let names = try await subjects()
        return "Reachable · \(names.count) subject\(names.count == 1 ? "" : "s")"
    }

    private func get(_ path: String, describing what: String) async throws -> Data {
        guard let url = URL(string: path, relativeTo: baseURL) else {
            throw SchemaRegistryError.badURL(baseURL.absoluteString + path)
        }

        var request = URLRequest(url: url)
        // The versioned type is what Schema Registry expects; it falls back to
        // JSON for anything else, but asking properly avoids surprises.
        request.setValue(
            "application/vnd.schemaregistry.v1+json, application/json",
            forHTTPHeaderField: "Accept"
        )
        if let authorization {
            request.setValue(authorization, forHTTPHeaderField: "Authorization")
        }

        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            throw SchemaRegistryError.unreachable(error.localizedDescription)
        }

        guard let http = response as? HTTPURLResponse else {
            throw SchemaRegistryError.malformedResponse("not an HTTP reply")
        }

        switch http.statusCode {
        case 200..<300:
            return data
        case 401, 403:
            throw SchemaRegistryError.unauthorized
        case 404:
            throw SchemaRegistryError.notFound(what)
        default:
            // The registry puts a readable message in the body; prefer it to
            // the bare status code.
            let message = (try? JSONSerialization.jsonObject(with: data) as? [String: Any])
                .flatMap { $0?["message"] as? String }
                ?? String(data: data, encoding: .utf8)
                ?? "no detail"
            throw SchemaRegistryError.http(status: http.statusCode, message: message)
        }
    }
}

/// An Avro payload decoded for display.
public struct DecodedAvro: Sendable, Equatable {
    public let schemaID: Int32
    /// The record as pretty-printed JSON.
    public let json: String
    /// The schema document, for the schema tab.
    public let schemaText: String

    public init(schemaID: Int32, json: String, schemaText: String) {
        self.schemaID = schemaID
        self.json = json
        self.schemaText = schemaText
    }
}

/// Turns Confluent-framed Avro records into readable JSON.
public enum AvroPayload {
    /// Whether a payload is Confluent-framed, and so worth trying to decode.
    public static func isFramed(_ payload: Data?) -> Bool {
        guard let payload else { return false }
        return ConfluentWireFormat.split(payload) != nil
    }

    /// The schema id a framed payload names, without fetching anything.
    public static func schemaID(of payload: Data?) -> Int32? {
        guard let payload else { return nil }
        return ConfluentWireFormat.split(payload)?.schemaID
    }

    /// Decodes a framed payload, fetching its schema from the registry.
    ///
    /// - Parameters:
    ///   - payload: the record's bytes, including the Confluent header.
    ///   - registry: client for the cluster's registry, or `nil` when none is
    ///     configured — which is reported as
    ///     ``SchemaRegistryError/notConfigured`` rather than silently falling
    ///     back to hex.
    /// - Returns: the decoded record, or `nil` if the payload is not framed.
    public static func decode(
        _ payload: Data?,
        registry: SchemaRegistryClient?
    ) async throws -> DecodedAvro? {
        guard let payload, let (id, body) = ConfluentWireFormat.split(payload) else { return nil }
        guard let registry else { throw SchemaRegistryError.notConfigured }

        let registered = try await registry.schema(id: id)
        let parsed = try await registry.parsedSchema(id: id)
        let value = try AvroDecoder(named: parsed.named).decode(body, as: parsed.schema)

        let data = try JSONSerialization.data(
            withJSONObject: value,
            options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes, .fragmentsAllowed]
        )
        return DecodedAvro(
            schemaID: id,
            json: String(data: data, encoding: .utf8) ?? "",
            schemaText: registered.text
        )
    }

    /// Encodes a JSON document as Avro for a subject's newest schema.
    ///
    /// - Parameters:
    ///   - json: the value to send, as JSON text.
    ///   - subject: subject whose latest schema to encode against.
    ///   - registry: the cluster's registry client.
    /// - Returns: Confluent-framed bytes, ready to produce.
    public static func encode(
        json: String,
        subject: String,
        registry: SchemaRegistryClient
    ) async throws -> Data {
        let registered = try await registry.latestSchema(subject: subject)
        let parsed = try AvroSchemaParser.parse(registered.text)

        let value = try JSONSerialization.jsonObject(
            with: Data(json.utf8),
            options: [.fragmentsAllowed]
        )
        let body = try AvroEncoder(named: parsed.named).encode(value, as: parsed.schema)
        return ConfluentWireFormat.frame(schemaID: registered.id, body: body)
    }
}
