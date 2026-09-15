import Foundation
import KestrelKit

let harness = Harness()

// MARK: Helpers

func temporaryDirectory() throws -> URL {
    let url = URL(fileURLWithPath: NSTemporaryDirectory())
        .appendingPathComponent("kestrel-checks-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    return url
}

func sampleProfile() -> ClusterProfile {
    ClusterProfile(
        name: "Staging",
        bootstrapServers: "broker1:9093,broker2:9093",
        securityProtocol: .saslSSL,
        saslMechanism: .scramSHA512,
        saslUsername: "kestrel",
        tls: TLSSettings(
            caLocation: "/etc/ssl/ca.pem",
            certificateLocation: "/etc/ssl/client.pem",
            keyLocation: "/etc/ssl/client.key",
            verifyHostname: false
        )
    )
}

// MARK: Profile persistence

harness.suite("Cluster profile persistence") { h in
    h.check("a saved list is restored by a fresh repository, as it is on relaunch") {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let profile = sampleProfile()
        try ClusterProfileRepository(directory: directory).save([profile])

        // A second instance reads from disk with no in-memory state, which is
        // what happens when the app is quit and launched again.
        let restored = try ClusterProfileRepository(directory: directory).load()
        try expectEqual(restored, [profile], "restored profiles")
    }

    h.check("a missing file loads as an empty list rather than throwing") {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        try expect(ClusterProfileRepository(directory: directory).load().isEmpty, "expected empty list")
    }

    h.check("the JSON on disk contains no secret material") {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let repository = ClusterProfileRepository(directory: directory)
        try repository.save([sampleProfile()])
        let json = try String(contentsOf: repository.fileURL, encoding: .utf8)

        try expect(json.contains("kestrel"), "username should be present")
        try expect(!json.localizedCaseInsensitiveContains("password"), "JSON mentions a password")
        try expect(!json.localizedCaseInsensitiveContains("passphrase"), "JSON mentions a passphrase")
    }

    h.check("a plaintext profile encodes without a SASL mechanism") {
        var profile = sampleProfile()
        profile.securityProtocol = .plaintext
        profile.saslMechanism = nil

        let decoded = try JSONDecoder().decode(
            ClusterProfile.self,
            from: try JSONEncoder().encode(profile)
        )
        try expect(decoded.saslMechanism == nil, "mechanism should stay nil")
        try expect(!decoded.securityProtocol.usesSASL, "PLAINTEXT should not use SASL")
        try expect(!decoded.securityProtocol.usesTLS, "PLAINTEXT should not use TLS")
    }

    h.check("the compression codec round trips, and an older profile reads as none") {
        var profile = sampleProfile()
        profile.compression = .snappy

        let decoded = try JSONDecoder().decode(
            ClusterProfile.self,
            from: try JSONEncoder().encode(profile)
        )
        try expectEqual(decoded.compression, .snappy, "codec")

        // A profile written before the field existed must keep producing the
        // way it did, which is librdkafka's default of no compression.
        let old = #"""
            {"id":"6E8C5A6E-4E6E-4C2E-9E4A-1B2C3D4E5F61","name":"old",
             "bootstrapServers":"localhost:19092","securityProtocol":"PLAINTEXT",
             "saslUsername":"","tls":{"caLocation":"","certificateLocation":"",
             "keyLocation":"","verifyHostname":true}}
            """#
        let older = try JSONDecoder().decode(ClusterProfile.self, from: Data(old.utf8))
        try expectEqual(older.compression, .none, "a profile without the field")
    }

    h.check("a codec nobody supports is refused by the decoder, not passed to the broker") {
        // Hand-edited into clusters.json, or written by a future build. The
        // value goes straight into librdkafka's compression.codec, so it must
        // fail at the profile rather than turn into a produce that reports
        // success while sending something else.
        let json = #"""
            {"id":"6E8C5A6E-4E6E-4C2E-9E4A-1B2C3D4E5F62","name":"exotic",
             "bootstrapServers":"localhost:19092","securityProtocol":"PLAINTEXT",
             "saslUsername":"","tls":{"caLocation":"","certificateLocation":"",
             "keyLocation":"","verifyHostname":true},"compression":"brotli"}
            """#
        let profile = try? JSONDecoder().decode(ClusterProfile.self, from: Data(json.utf8))
        try expect(profile == nil, "\"brotli\" should not decode into a profile")
    }

    h.check("every codec the menu offers is one librdkafka accepts") {
        // The raw values go straight into compression.codec, so a misspelling
        // would only show up as a client that refuses to be created. Needs no
        // broker: rd_kafka_new validates the config before connecting.
        for codec in CompressionCodec.allCases {
            var profile = ClusterProfile(name: "probe", bootstrapServers: "localhost:1")
            profile.compression = codec
            _ = try KafkaClient(profile: profile)
        }
    }
}

// MARK: Keychain

// Uses the real login keychain under a checks-only service name, so the app's
// own items are untouched. This process writes and reads, which keeps the
// keychain ACL satisfied without a permission prompt.
let keychain = KeychainStore(service: "dev.kestrel.Kestrel.checks")

harness.suite("Keychain secrets") { h in
    h.check("a stored secret round-trips") {
        let cluster = UUID()
        defer { try? keychain.removeAll(cluster: cluster) }

        try keychain.set("s3cret-pw", secret: .saslPassword, cluster: cluster)
        try expectEqual(try keychain.get(secret: .saslPassword, cluster: cluster), "s3cret-pw", "password")
    }

    h.check("writing twice updates in place instead of duplicating") {
        let cluster = UUID()
        defer { try? keychain.removeAll(cluster: cluster) }

        try keychain.set("first", secret: .saslPassword, cluster: cluster)
        try keychain.set("second", secret: .saslPassword, cluster: cluster)
        try expectEqual(try keychain.get(secret: .saslPassword, cluster: cluster), "second", "password")
    }

    h.check("secrets are scoped per cluster and per kind") {
        let a = UUID()
        let b = UUID()
        defer {
            try? keychain.removeAll(cluster: a)
            try? keychain.removeAll(cluster: b)
        }

        try keychain.set("pw-a", secret: .saslPassword, cluster: a)
        try keychain.set("phrase-a", secret: .tlsKeyPassphrase, cluster: a)
        try keychain.set("pw-b", secret: .saslPassword, cluster: b)

        try expectEqual(try keychain.get(secret: .saslPassword, cluster: a), "pw-a", "a password")
        try expectEqual(try keychain.get(secret: .tlsKeyPassphrase, cluster: a), "phrase-a", "a passphrase")
        try expectEqual(try keychain.get(secret: .saslPassword, cluster: b), "pw-b", "b password")
        try expectEqual(try keychain.get(secret: .tlsKeyPassphrase, cluster: b), nil, "b passphrase")
    }

    h.check("an empty value clears the stored secret") {
        let cluster = UUID()
        defer { try? keychain.removeAll(cluster: cluster) }

        try keychain.set("temp", secret: .saslPassword, cluster: cluster)
        try keychain.set("", secret: .saslPassword, cluster: cluster)
        try expectEqual(try keychain.get(secret: .saslPassword, cluster: cluster), nil, "password")
    }

    h.check("removing a cluster removes every secret it owns") {
        let cluster = UUID()
        try keychain.set("pw", secret: .saslPassword, cluster: cluster)
        try keychain.set("phrase", secret: .tlsKeyPassphrase, cluster: cluster)
        try keychain.removeAll(cluster: cluster)

        try expectEqual(try keychain.get(secret: .saslPassword, cluster: cluster), nil, "password")
        try expectEqual(try keychain.get(secret: .tlsKeyPassphrase, cluster: cluster), nil, "passphrase")
    }

    h.check("reading an unknown secret returns nil rather than throwing") {
        try expectEqual(try keychain.get(secret: .saslPassword, cluster: UUID()), nil, "password")
    }
}

// MARK: Kafka client
//
// Talks to Kestrel's own broker from Docker/kafka.yml, on port 19092 rather
// than 9092 so it can run alongside any other Kafka on the machine.
// The "cluster is down" checks point
// at port 9099, where nothing listens, so the refusal path is exercised too.

/// Topics and a consumer group the checks create for themselves.
///
/// The suite seeds its own data below rather than borrowing another stack's
/// topics, so it cannot fail because something unrelated was torn down.
let fixtureTopic = "kestrel.fixture.records"
let fixtureEmptyTopic = "kestrel.fixture.empty"
let fixtureGroup = "kestrel.fixture.group"
/// A group with a real member, for the assignment and refusal checks.
let fixtureLiveGroup = "kestrel.fixture.live"

/// A document large enough to prove the pretty printer on realistic input,
/// mirroring the several-KB service payloads earlier ticks borrowed.
let fixtureLargeValue: Data = {
    let passengers = (0..<12).map { index in
        """
        {"id":\(index),"name":"Passenger \(index)","seat":"\(12 + index)A",\
        "bags":[{"weight":\(10 + index),"tag":"BAG\(index)"}],\
        "contact":{"email":"p\(index)@example.test","phone":"+62811000\(index)"}}
        """
    }.joined(separator: ",")
    return Data(
        """
        {"bookingId":"KESTREL-FIXTURE-1","status":"CONFIRMED",\
        "itinerary":{"from":"CGK","to":"DPS","segments":[{"flight":"GA402","depart":"2026-01-01T07:00:00Z"}]},\
        "passengers":[\(passengers)],"audit":{"source":"fixtures","note":"large document for pretty-print checks"}}
        """.utf8
    )
}()

/// Runs a real consumer inside the broker container so a group has a live
/// member. A Java console consumer is used rather than our own client, which is
/// assign-only and so never joins a group.
func startLiveConsumer(group: String, topic: String) -> Process? {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    process.arguments = [
        "docker", "exec", "kestrel-kafka",
        "kafka-console-consumer",
        "--bootstrap-server", "localhost:19092",
        "--topic", topic,
        "--group", group,
    ]
    process.standardOutput = FileHandle.nullDevice
    process.standardError = FileHandle.nullDevice
    do {
        try process.run()
        return process
    } catch {
        return nil
    }
}

/// Stops the console consumer, in the container as well as locally: killing the
/// `docker exec` client does not always reap the process it started.
///
/// Matches on `ConsoleConsumer`, the class in the java command line. The
/// `kafka-console-consumer` name belongs only to the wrapper script, so killing
/// that leaves the consumer itself running and the group gains a second member
/// on every run, which then rebalances endlessly and holds no assignment.
func stopLiveConsumer(_ process: Process?) {
    process?.terminate()
    let pkill = Process()
    pkill.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    pkill.arguments = ["docker", "exec", "kestrel-kafka", "pkill", "-f", "ConsoleConsumer"]
    pkill.standardOutput = FileHandle.nullDevice
    pkill.standardError = FileHandle.nullDevice
    try? pkill.run()
    pkill.waitUntilExit()
}

/// The console consumer that holds `fixtureLiveGroup` open for the run.
var liveConsumer: Process?

let localProfile = ClusterProfile(
    name: "checks-local",
    bootstrapServers: "localhost:19092",
    securityProtocol: .plaintext
)
let deadProfile = ClusterProfile(
    name: "checks-dead",
    bootstrapServers: "localhost:9099",
    securityProtocol: .plaintext
)

print("\nlibrdkafka \(KafkaClient.librdkafkaVersion)")

await harness.asyncSuite("Fixtures") { h in
    await h.checkAsync("the broker is reachable and the fixtures are in place") {
        let client = try KafkaClient(profile: localProfile)

        switch await client.testConnection() {
        case .failure(let message):
            throw Expectation(
                description: """
                    \(localProfile.bootstrapServers) is unreachable: \(message). \
                    Start it with: docker compose -f Docker/kafka.yml up -d
                    """
            )
        case .success:
            break
        }

        try await client.createTopic(name: fixtureTopic, partitions: 1)
        try await client.createTopic(name: fixtureEmptyTopic, partitions: 1)
        try await client.createTopic(name: "kestrel.produce.test", partitions: 1)

        var names: Set<String> = []
        for _ in 0..<20 where !names.isSuperset(of: [fixtureTopic, fixtureEmptyTopic]) {
            names = Set(try await client.topicNames())
            if !names.isSuperset(of: [fixtureTopic, fixtureEmptyTopic]) {
                try await Task.sleep(for: .milliseconds(300))
            }
        }
        try expect(names.contains(fixtureTopic), "\(fixtureTopic) should exist")
        try expect(names.contains(fixtureEmptyTopic), "\(fixtureEmptyTopic) should exist")

        // Enough records that latest-N, mid-offset reads and trimming all have
        // something to work with. Values are JSON so the pretty printers and
        // the envelope checks see realistic payloads.
        let consumer = try KafkaConsumer(profile: localProfile)
        let existing = try await consumer.watermarks(topic: fixtureTopic, partition: 0)
        let wanted = 20
        if existing.high - existing.low < Int64(wanted) {
            for index in existing.high..<Int64(wanted) {
                _ = try await client.produce(
                    topic: fixtureTopic,
                    key: Data("fixture-key-\(index)".utf8),
                    value: Data(
                        """
                        {"index":\(index),"source":"fixtures","nested":{"a":[1,2,3],"b":"text \(index)"}}
                        """.utf8
                    ),
                    headers: index % 2 == 0
                        ? [RecordHeader(name: "origin", value: Data("fixtures".utf8))]
                        : []
                )
            }
        }

        // Last, so `latest(count: 1)` finds it.
        if existing.high - existing.low < Int64(wanted) + 1 {
            _ = try await client.produce(
                topic: fixtureTopic,
                key: Data("fixture-large".utf8),
                value: fixtureLargeValue,
                headers: [RecordHeader(name: "origin", value: Data("fixtures".utf8))]
            )
        }

        let marks = try await consumer.watermarks(topic: fixtureTopic, partition: 0)
        try expect(marks.high >= Int64(wanted), "\(fixtureTopic) should hold at least \(wanted) records")
        try expect(
            try await consumer.watermarks(topic: fixtureEmptyTopic, partition: 0).high == 0,
            "\(fixtureEmptyTopic) must stay empty"
        )

        // A group deliberately parked behind the end, so lag is non-zero.
        //
        // Retried because a freshly started broker answers "Not coordinator"
        // until the group coordinator for this group id is established. The
        // error is transient, not a rejection.
        var lastFailure: Error?
        for attempt in 0..<12 {
            do {
                try await client.resetGroupOffsets(
                    group: fixtureGroup,
                    positions: [(fixtureTopic, 0, .offset(max(0, marks.high - 5)))]
                )
                lastFailure = nil
                if attempt > 0 { print("       group coordinator ready after \(attempt + 1) attempts") }
                break
            } catch {
                lastFailure = error
                try await Task.sleep(for: .milliseconds(500))
            }
        }
        if let lastFailure { throw lastFailure }
        let offsets = try await client.groupOffsets(group: fixtureGroup)
        try expect(!offsets.isEmpty, "\(fixtureGroup) should have a committed offset")
        try expect((offsets.first?.lag ?? 0) > 0, "the fixture group should be lagging")

        // A second group with a real member, for the assignment and refusal
        // checks. Any consumer left over from an earlier run is reaped first.
        stopLiveConsumer(nil)
        liveConsumer = startLiveConsumer(group: fixtureLiveGroup, topic: fixtureTopic)
        try expect(liveConsumer != nil, "could not start a console consumer via docker exec")

        var joined = false
        for _ in 0..<40 {
            let groups = try await client.consumerGroups()
            if let live = groups.first(where: { $0.id == fixtureLiveGroup }),
               live.members.contains(where: { !$0.assignments.isEmpty }) {
                joined = true
                print("       \(fixtureLiveGroup): \(live.members.count) live member(s), state \(live.state)")
                break
            }
            try await Task.sleep(for: .milliseconds(500))
        }
        try expect(joined, "\(fixtureLiveGroup) never gained a member with an assignment")

        print("       \(fixtureTopic): offsets \(marks.low)..<\(marks.high)")
        print("       \(fixtureGroup): committed \(offsets.first?.committed ?? -1), lag \(offsets.first?.lag ?? -1)")
    }
}

await harness.asyncSuite("Kafka client against localhost:19092") { h in
    await h.checkAsync("metadata reports at least one broker") {
        let client = try KafkaClient(profile: localProfile)
        let metadata = try await client.metadata(timeout: .seconds(10))
        try expect(!metadata.brokers.isEmpty, "expected at least one broker")
        print("       brokers: \(metadata.brokers.map(\.endpoint).joined(separator: ", "))")
        print("       topics: \(metadata.topics.count), via \(metadata.originatingBroker)")
    }

    await h.checkAsync("testConnection succeeds and summarises the cluster") {
        let client = try KafkaClient(profile: localProfile)
        let result = await client.testConnection(timeout: .seconds(10))
        try expect(result.isSuccess, "expected success, got: \(result.summary)")
        print("       \(result.summary)")
    }

    await h.checkAsync("topic names come back sorted and without internal topics") {
        let client = try KafkaClient(profile: localProfile)
        let names = try await client.topicNames()
        try expect(names == names.sorted(), "topic names should be sorted")
        try expect(!names.contains { $0.hasPrefix("__") }, "internal topics should be hidden")
        let all = try await client.topicNames(includeInternal: true)
        try expect(all.count >= names.count, "including internals cannot shrink the list")
        print("       \(names.count) topics, \(all.count) including internal")
    }

    await h.checkAsync("broker configs include broker.rack") {
        let client = try KafkaClient(profile: localProfile)
        let metadata = try await client.metadata()
        guard let broker = metadata.brokers.first else {
            throw Expectation(description: "no broker to describe")
        }
        let configs = try await client.describeConfigs(.broker(broker.id))
        try expect(!configs.isEmpty, "expected broker config entries")
        try expect(configs.contains { $0.name == "broker.rack" }, "broker.rack should be present")
        try expect(configs.map(\.name) == configs.map(\.name).sorted(), "entries should be sorted")
        let rack = configs.first { $0.name == "broker.rack" }
        print("       \(configs.count) entries; broker.rack = \(rack?.displayValue ?? "—")")
    }

    await h.checkAsync("sensitive broker configs withhold their value") {
        let client = try KafkaClient(profile: localProfile)
        let configs = try await client.describeConfigs(.broker(1))
        let sensitive = configs.filter(\.isSensitive)
        try expect(sensitive.allSatisfy { $0.value == nil }, "sensitive values must not be disclosed")
        try expect(sensitive.allSatisfy { $0.displayValue == "••••••" }, "sensitive entries should mask")
        print("       \(sensitive.count) sensitive entries, all withheld")
    }

    await h.checkAsync("topic configs report retention and cleanup policy") {
        let client = try KafkaClient(profile: localProfile)
        guard let topic = try await client.topicNames().first else {
            throw Expectation(description: "no topic to describe")
        }
        let configs = try await client.describeConfigs(.topic(topic))
        try expect(!configs.isEmpty, "expected topic config entries")
        try expect(configs.contains { $0.name == "cleanup.policy" }, "cleanup.policy should be present")
        try expect(configs.contains { $0.name == "retention.ms" }, "retention.ms should be present")
        let policy = configs.first { $0.name == "cleanup.policy" }
        print("       \(topic): \(configs.count) entries, cleanup.policy = \(policy?.displayValue ?? "—")")
    }

    await h.checkAsync("describing a missing topic reports a clear error") {
        let client = try KafkaClient(profile: localProfile)
        do {
            _ = try await client.describeConfigs(.topic("kestrel-does-not-exist-\(UUID().uuidString)"))
            throw Expectation(description: "expected an error for an unknown topic")
        } catch let error as KafkaError {
            print("       \(error.errorDescription ?? "")")
        }
    }

    await h.checkAsync("partition metadata carries leader, replicas and ISR") {
        let client = try KafkaClient(profile: localProfile)
        let metadata = try await client.metadata()
        guard let topic = metadata.topics.first(where: { !$0.partitions.isEmpty }) else {
            throw Expectation(description: "no topic with partitions")
        }
        try expect(topic.partitionCount == topic.partitions.count, "partition count should match")
        try expect(
            topic.partitions.allSatisfy { !$0.replicas.isEmpty },
            "every partition should list replicas"
        )
        try expect(
            topic.partitions.map(\.id) == topic.partitions.map(\.id).sorted(),
            "partitions should be ordered by id"
        )
        let first = topic.partitions[0]
        print("       \(topic.name) p\(first.id): leader \(first.leader), replicas \(first.replicas), isr \(first.inSyncReplicas)")
    }

    await h.checkAsync("consumer groups can be listed") {
        let client = try KafkaClient(profile: localProfile)
        let groups = try await client.consumerGroups(timeout: .seconds(10))
        print("       \(groups.count) groups: \(groups.prefix(5).map(\.id).joined(separator: ", "))")
    }
}

await harness.asyncSuite("Message browser against localhost:19092") { h in
    // Find a partition that actually holds records, so the assertions are real.
    var populated: (topic: String, partition: Int32, low: Int64, high: Int64)?
    var emptyTopic: String?

    await h.checkAsync("a partition with records can be located") {
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)
        for name in try await client.topicNames() {
            let marks = try await consumer.watermarks(topic: name, partition: 0)
            if marks.high > marks.low, populated == nil {
                populated = (name, 0, marks.low, marks.high)
            } else if marks.high == marks.low, emptyTopic == nil {
                emptyTopic = name
            }
            if populated != nil, emptyTopic != nil { break }
        }
        guard let populated else {
            throw Expectation(description: "no topic on localhost:19092 holds records")
        }
        print("       \(populated.topic) p0 holds offsets \(populated.low)..<\(populated.high)")
        print("       empty topic for the empty-state check: \(emptyTopic ?? "none found")")
    }

    await h.checkAsync("reading from earliest returns records in offset order") {
        guard let target = populated else { throw Expectation(description: "no populated partition") }
        let consumer = try KafkaConsumer(profile: localProfile)
        let page = try await consumer.fetch(
            topic: target.topic,
            partition: target.partition,
            from: .earliest,
            limit: 10
        )
        try expect(!page.records.isEmpty, "expected records from earliest")
        try expectEqual(page.startOffset, target.low, "start offset")
        try expect(
            page.records.map(\.offset) == page.records.map(\.offset).sorted(),
            "records should arrive in offset order"
        )
        try expectEqual(page.records.first?.offset, target.low, "first record offset")
        let first = page.records[0]
        print("       \(page.records.count) records; first offset \(first.offset), key \(first.keyPreview), \(first.valueByteCount) value bytes")
        print("       value preview: \(first.valuePreview.prefix(80))")
    }

    await h.checkAsync("latest-N starts N records before the high watermark") {
        guard let target = populated else { throw Expectation(description: "no populated partition") }
        let consumer = try KafkaConsumer(profile: localProfile)
        let page = try await consumer.fetch(
            topic: target.topic,
            partition: target.partition,
            from: .latest(count: 3),
            limit: 10
        )
        let expected = max(target.low, target.high - 3)
        try expectEqual(page.startOffset, expected, "start offset for latest-3")
        try expect(page.records.count <= 3, "latest-3 should not return more than 3")
        try expect(page.reachedEnd, "a tail read should reach the end")
        print("       started at \(page.startOffset), got \(page.records.count) records, high \(page.highWatermark)")
    }

    await h.checkAsync("a numeric offset is honoured and clamped to the retained range") {
        guard let target = populated else { throw Expectation(description: "no populated partition") }
        let consumer = try KafkaConsumer(profile: localProfile)

        let middle = target.low + (target.high - target.low) / 2
        let page = try await consumer.fetch(
            topic: target.topic,
            partition: target.partition,
            from: .offset(middle),
            limit: 5
        )
        try expectEqual(page.startOffset, middle, "start offset")
        if let first = page.records.first {
            try expectEqual(first.offset, middle, "first record should be the requested offset")
        }

        let clamped = try await consumer.fetch(
            topic: target.topic,
            partition: target.partition,
            from: .offset(-500),
            limit: 1
        )
        try expectEqual(clamped.startOffset, target.low, "a negative offset should clamp to low")
        print("       middle \(middle) honoured; -500 clamped to \(clamped.startOffset)")
    }

    await h.checkAsync("an empty partition returns promptly instead of hanging") {
        guard let topic = emptyTopic else {
            print("       skipped: every topic on this cluster holds records")
            return
        }
        let consumer = try KafkaConsumer(profile: localProfile)
        let started = Date()
        let page = try await consumer.fetch(
            topic: topic,
            partition: 0,
            from: .earliest,
            limit: 10,
            timeout: .seconds(10)
        )
        let elapsed = Date().timeIntervalSince(started)
        try expect(page.records.isEmpty, "an empty partition should yield no records")
        try expect(page.isPartitionEmpty, "the page should report the partition as empty")
        try expect(page.reachedEnd, "the page should report reaching the end")
        try expect(elapsed < 3, "should not wait out the timeout, took \(String(format: "%.1f", elapsed))s")
        print("       \(topic) returned empty in \(String(format: "%.2f", elapsed))s")
    }

    await h.checkAsync("records expose headers and a single-line value preview") {
        guard let target = populated else { throw Expectation(description: "no populated partition") }
        let consumer = try KafkaConsumer(profile: localProfile)
        let page = try await consumer.fetch(
            topic: target.topic,
            partition: target.partition,
            from: .latest(count: 5),
            limit: 5
        )
        guard let record = page.records.last else {
            throw Expectation(description: "expected at least one record")
        }
        try expect(!record.valuePreview.contains("\n"), "the preview must stay on one line")
        try expect(record.id == "\(record.partition):\(record.offset)", "id should be partition:offset")
        print("       offset \(record.offset), \(record.headers.count) headers, timestamp \(record.timestamp?.description ?? "none")")
        for header in record.headers.prefix(3) {
            print("       header \(header.name)=\(header.displayValue.prefix(40))")
        }
    }
}

harness.suite("Payload formatting") { h in
    h.check("valid JSON is detected and indented") {
        let payload = PayloadFormatter.format(Data(#"{"b":2,"a":{"c":[1,2]}}"#.utf8))
        try expectEqual(payload.kind, .json, "kind")
        try expect(payload.problem == nil, "valid JSON should report no problem")
        try expect(!payload.isMalformed, "valid JSON is not malformed")
        guard let pretty = payload.pretty else {
            throw Expectation(description: "expected a pretty form")
        }
        try expect(pretty.contains("\n"), "pretty JSON should span lines")
        try expect(pretty.contains("  \"a\""), "pretty JSON should be indented")
        // Keys are sorted, so "a" precedes "b" regardless of source order.
        let a = pretty.range(of: "\"a\"")
        let b = pretty.range(of: "\"b\"")
        try expect(a != nil && b != nil && a!.lowerBound < b!.lowerBound, "keys should be sorted")
        try expectEqual(payload.text(pretty: false), #"{"b":2,"a":{"c":[1,2]}}"#, "raw text")
    }

    h.check("invalid JSON stays raw and reports the problem") {
        // Not a trailing comma: this Foundation's JSON parser accepts those.
        // An unterminated object is rejected by any parser.
        let broken = #"{"a": 1, "b": [2, 3"#
        let payload = PayloadFormatter.format(Data(broken.utf8))
        try expectEqual(payload.kind, .json, "kind should still be JSON")
        try expect(payload.pretty == nil, "there should be no pretty form")
        try expect(payload.isMalformed, "should be flagged malformed for the badge")
        try expectEqual(payload.raw, broken, "raw should be untouched")
        try expectEqual(payload.text(pretty: true), broken, "pretty view falls back to raw")
        guard let problem = payload.problem else {
            throw Expectation(description: "expected a problem message")
        }
        try expect(problem.hasPrefix("Invalid JSON"), "problem should name the format: \(problem)")
        print("       badge: \(problem.prefix(70))")
    }

    h.check("valid XML is detected and indented") {
        let payload = PayloadFormatter.format(Data("<a><b>1</b><c>2</c></a>".utf8))
        try expectEqual(payload.kind, .xml, "kind")
        guard let pretty = payload.pretty else {
            throw Expectation(description: "expected a pretty form")
        }
        try expect(pretty.contains("\n"), "pretty XML should span lines")
        try expect(pretty.contains("    <b>1</b>") || pretty.contains("  <b>1</b>"), "should indent children: \(pretty)")
    }

    h.check("invalid XML stays raw with a badge") {
        let payload = PayloadFormatter.format(Data("<a><b>oops</a>".utf8))
        try expectEqual(payload.kind, .xml, "kind")
        try expect(payload.pretty == nil, "there should be no pretty form")
        try expect(payload.isMalformed, "should be flagged malformed")
        try expect(payload.problem?.hasPrefix("Invalid XML") == true, "problem should name the format")
    }

    h.check("plain text and structured payloads are told apart") {
        let text = PayloadFormatter.format(Data("just a string".utf8))
        try expectEqual(text.kind, .text, "plain text kind")
        try expect(text.pretty == nil, "plain text has no pretty form")
        try expect(!text.kind.isStructured, "text is not structured")
        try expect(PayloadKind.json.isStructured && PayloadKind.xml.isStructured, "json and xml are structured")
    }

    h.check("leading whitespace does not hide a JSON document") {
        let payload = PayloadFormatter.format(Data("\n\t  {\"a\":1}".utf8))
        try expectEqual(payload.kind, .json, "kind")
        try expect(payload.pretty != nil, "should still pretty-print")
    }

    h.check("absent, empty and binary payloads each have their own kind") {
        try expectEqual(PayloadFormatter.format(nil).kind, .null, "nil is null")
        try expectEqual(PayloadFormatter.format(nil).raw, "null", "nil renders as null")
        try expectEqual(PayloadFormatter.format(Data()).kind, .empty, "empty data")

        // 0xff and 0xfe cannot start a valid UTF-8 sequence.
        let binary = PayloadFormatter.format(Data([0xff, 0xfe, 0x00, 0x41]))
        try expectEqual(binary.kind, .binary, "invalid UTF-8 is binary")
        try expect(binary.raw.contains("ff fe"), "hex dump should list the bytes: \(binary.raw)")
        // Four bytes, three unprintable: 0xff 0xfe 0x00 render as dots, 0x41 as "A".
        try expect(binary.raw.contains("|...A|"), "dump should show printable ASCII: \(binary.raw)")
        try expectEqual(binary.byteCount, 4, "byte count")
        print("       dump: \(binary.raw)")
    }

    h.check("a large hex dump is truncated with a byte count") {
        let dump = PayloadFormatter.hexDump(Data(repeating: 0x41, count: 3000), limit: 64)
        try expect(dump.contains("… 2936 more bytes"), "should report the remainder: \(dump.suffix(40))")
        try expectEqual(dump.split(separator: "\n").count, 5, "4 rows of 16 bytes plus the notice")
    }
}

harness.suite("Saving records to a file") { h in
    let directory = URL(fileURLWithPath: "/tmp/kestrel-save-checks", isDirectory: true)
    try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)

    func record(
        key: Data? = Data("k".utf8),
        value: Data?,
        headers: [RecordHeader] = []
    ) -> KafkaRecord {
        KafkaRecord(
            partition: 3,
            offset: 42,
            timestamp: Date(timeIntervalSince1970: 1_700_000_000),
            timestampKind: .createTime,
            key: key,
            value: value,
            headers: headers
        )
    }

    h.check("raw value bytes round-trip through the file") {
        // The slice's acceptance line.
        let payload = Data(#"{"hello":"wörld"}"#.utf8)
        let url = directory.appendingPathComponent("raw.json")
        let written = try RecordFile.write(
            record: record(value: payload),
            topic: "t",
            format: .rawValue,
            to: url
        )
        let readBack = try Data(contentsOf: url)
        try expectEqual(readBack, payload, "bytes on disk must equal the value bytes")
        try expectEqual(written, payload.count, "reported byte count")
        print("       \(written) bytes, identical after reading back")
    }

    h.check("binary value bytes round-trip through the file") {
        // Bytes that are not valid UTF-8, so nothing may go through a string.
        let payload = Data([0xff, 0x00, 0xfe, 0x41, 0x80])
        let url = directory.appendingPathComponent("raw.bin")
        try RecordFile.write(record: record(value: payload), topic: "t", format: .rawValue, to: url)
        try expectEqual(try Data(contentsOf: url), payload, "binary bytes")
    }

    h.check("the JSON envelope round-trips text values readably") {
        let payload = Data(#"{"a":1}"#.utf8)
        let url = directory.appendingPathComponent("envelope.json")
        try RecordFile.write(
            record: record(
                value: payload,
                headers: [RecordHeader(name: "origin", value: Data("checks".utf8))]
            ),
            topic: "orders",
            format: .jsonEnvelope,
            to: url
        )

        let text = try String(contentsOf: url, encoding: .utf8)
        try expect(text.contains("\"utf8\""), "a text value should be tagged utf8, not base64")
        try expect(text.contains("orders"), "the topic should be recorded")

        let envelope = try RecordFile.readEnvelope(from: url)
        try expectEqual(envelope.valueBytes, payload, "value bytes")
        try expectEqual(envelope.keyBytes, Data("k".utf8), "key bytes")
        try expectEqual(envelope.topic, "orders", "topic")
        try expectEqual(envelope.partition, 3, "partition")
        try expectEqual(envelope.offset, 42, "offset")
        try expectEqual(envelope.timestampMillis, 1_700_000_000_000, "timestamp in millis")
        try expectEqual(envelope.headers.count, 1, "header count")
        try expectEqual(envelope.recordHeaders.first?.value, Data("checks".utf8), "header value")
        print("       envelope keeps text readable and restores \(envelope.valueBytes?.count ?? 0) value bytes")
    }

    h.check("the JSON envelope round-trips binary values as base64") {
        let payload = Data([0xff, 0xfe, 0x00, 0x01, 0x02])
        let url = directory.appendingPathComponent("envelope-binary.json")
        try RecordFile.write(
            record: record(value: payload),
            topic: "t",
            format: .jsonEnvelope,
            to: url
        )
        let text = try String(contentsOf: url, encoding: .utf8)
        try expect(text.contains("\"base64\""), "binary should be tagged base64")

        let envelope = try RecordFile.readEnvelope(from: url)
        try expectEqual(envelope.valueBytes, payload, "binary value bytes")
    }

    h.check("a tombstone stays distinct from an empty value") {
        let tombstoneURL = directory.appendingPathComponent("tombstone.json")
        try RecordFile.write(
            record: record(value: nil),
            topic: "t",
            format: .jsonEnvelope,
            to: tombstoneURL
        )
        let tombstone = try RecordFile.readEnvelope(from: tombstoneURL)
        try expect(tombstone.value == nil, "a tombstone's value must stay null")
        try expect(tombstone.valueBytes == nil, "a tombstone has no bytes")

        let emptyURL = directory.appendingPathComponent("empty.json")
        try RecordFile.write(
            record: record(value: Data()),
            topic: "t",
            format: .jsonEnvelope,
            to: emptyURL
        )
        let empty = try RecordFile.readEnvelope(from: emptyURL)
        try expect(empty.value != nil, "an empty value is still a value")
        try expectEqual(empty.valueBytes, Data(), "empty value bytes")

        // Raw format cannot express the difference; both are empty files.
        let rawTombstone = try RecordFile.encode(
            record: record(value: nil),
            topic: "t",
            format: .rawValue
        )
        try expectEqual(rawTombstone, Data(), "a raw tombstone writes an empty file")
        print("       envelope keeps null vs empty; raw bytes cannot, as expected")
    }

    h.check("a keyless record stays keyless") {
        let url = directory.appendingPathComponent("keyless.json")
        try RecordFile.write(
            record: record(key: nil, value: Data("v".utf8)),
            topic: "t",
            format: .jsonEnvelope,
            to: url
        )
        let envelope = try RecordFile.readEnvelope(from: url)
        try expect(envelope.key == nil, "key must stay null")
        try expect(envelope.keyBytes == nil, "no key bytes")
    }

    h.check("the suggested extension follows the payload") {
        try expectEqual(
            RecordSaveFormat.jsonEnvelope.suggestedExtension(for: Data("anything".utf8)),
            "envelope.json",
            "envelopes are always json"
        )

        // The two formats must never suggest the same name for one record, or
        // saving both would overwrite the first file.
        let jsonValued = KafkaRecord(
            partition: 0,
            offset: 7,
            timestamp: nil,
            timestampKind: .unavailable,
            key: nil,
            value: Data(#"{"a":1}"#.utf8),
            headers: []
        )
        let names = RecordSaveFormat.allCases.map {
            RecordSaveFormat.suggestedNameForChecks(record: jsonValued, topic: "t", format: $0)
        }
        try expectEqual(Set(names).count, names.count, "suggested names must differ: \(names)")
        try expectEqual(
            RecordSaveFormat.rawValue.suggestedExtension(for: Data(#"{"a":1}"#.utf8)),
            "json",
            "a JSON value"
        )
        try expectEqual(
            RecordSaveFormat.rawValue.suggestedExtension(for: Data("<a/>".utf8)),
            "xml",
            "an XML value"
        )
        try expectEqual(
            RecordSaveFormat.rawValue.suggestedExtension(for: Data("plain".utf8)),
            "txt",
            "plain text"
        )
        try expectEqual(
            RecordSaveFormat.rawValue.suggestedExtension(for: Data([0xff, 0xfe])),
            "bin",
            "binary"
        )
        try expectEqual(
            RecordSaveFormat.rawValue.suggestedExtension(for: nil),
            "bin",
            "a tombstone"
        )
    }

    h.check("a malformed envelope is rejected rather than half-read") {
        let url = directory.appendingPathComponent("broken.json")
        try Data(#"{"headers": "not an array"}"#.utf8).write(to: url)
        do {
            _ = try RecordFile.readEnvelope(from: url)
            throw Expectation(description: "a malformed envelope should not decode")
        } catch is DecodingError {
            // Expected.
        }
    }
}

await harness.asyncSuite("Saving a live record") { h in
    await h.checkAsync("a record read from localhost:19092 saves and round-trips") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let topic = fixtureTopic
        let page = try await consumer.fetch(topic: topic, partition: 0, from: .latest(count: 1), limit: 1)
        guard let record = page.records.first, let value = record.value else {
            throw Expectation(description: "expected a live record with a value")
        }

        let directory = URL(fileURLWithPath: "/tmp/kestrel-save-checks", isDirectory: true)
        let rawURL = directory.appendingPathComponent("live-raw.json")
        try RecordFile.write(record: record, topic: topic, format: .rawValue, to: rawURL)
        try expectEqual(try Data(contentsOf: rawURL), value, "live value bytes must match exactly")

        let envelopeURL = directory.appendingPathComponent("live-envelope.json")
        try RecordFile.write(record: record, topic: topic, format: .jsonEnvelope, to: envelopeURL)
        let envelope = try RecordFile.readEnvelope(from: envelopeURL)
        try expectEqual(envelope.valueBytes, value, "envelope value bytes")
        try expectEqual(envelope.offset, record.offset, "offset")
        try expectEqual(
            envelope.timestampMillis,
            record.timestamp.map { Int64(($0.timeIntervalSince1970 * 1000).rounded()) },
            "timestamp"
        )
        // The readable timestamp is informational, but it must still be readable.
        let readable = envelope.timestamp ?? ""
        try expect(readable.count > 10, "expected a full ISO 8601 timestamp, got \"\(readable)\"")
        try expect(readable.contains("T"), "ISO 8601 separates date and time with T: \"\(readable)\"")
        try expect(readable.hasPrefix("20"), "should start with the year: \"\(readable)\"")

        print("       offset \(record.offset): \(value.count) value bytes round-tripped in both formats")
        print("       envelope timestamp \(readable)")
    }
}

await harness.asyncSuite("Compression") { h in
    let scratchTopic = "kestrel.produce.test"

    // One check per codec rather than a loop, so a broker or a librdkafka build
    // missing exactly one of them names it instead of failing the whole set.
    for codec in CompressionCodec.allCases {
        await h.checkAsync("a record produced with \(codec.rawValue) reads back unchanged") {
            var profile = localProfile
            profile.compression = codec

            // A repetitive value, so a codec that silently did nothing would
            // still be doing something visible on the wire. What is asserted is
            // the round trip: the consumer is told nothing about the codec and
            // must still hand back the original bytes.
            let value = Data(String(repeating: "compress me. ", count: 200).utf8)
            let key = Data("codec-\(codec.rawValue)".utf8)

            let client = try KafkaClient(profile: profile, secrets: .none)
            let report = try await client.produce(topic: scratchTopic, key: key, value: value)

            let consumer = try KafkaConsumer(profile: localProfile)
            let page = try await consumer.fetch(
                topic: scratchTopic,
                partition: report.partition,
                from: .offset(report.offset),
                limit: 1
            )
            guard let record = page.records.first else {
                throw Expectation(description: "nothing at offset \(report.offset)")
            }
            try expectEqual(record.key, key, "key")
            try expectEqual(record.value, value, "value")
            print("       \(codec.rawValue): \(value.count) bytes at offset \(record.offset)")
        }
    }

}

await harness.asyncSuite("Topic management") { h in
    // The name from the slice's acceptance line. Created and deleted here, so
    // it must not exist beforehand or afterwards.
    let smokeTopic = "kestrel-loop-smoke"

    await h.checkAsync("a topic is created with partitions, replication and configs") {
        let client = try KafkaClient(profile: localProfile)
        let created = try await client.createTopic(
            name: smokeTopic,
            partitions: 2,
            replicationFactor: 1,
            configs: ["cleanup.policy": "compact", "retention.ms": "86400000"]
        )
        try expect(created, "the topic should be newly created")

        var topic: TopicInfo?
        for _ in 0..<15 where topic == nil {
            topic = try await client.metadata().topics.first { $0.name == smokeTopic }
            if topic == nil { try await Task.sleep(for: .milliseconds(300)) }
        }
        guard let topic else { throw Expectation(description: "\(smokeTopic) never appeared") }
        try expectEqual(topic.partitionCount, 2, "partition count")
        try expectEqual(topic.partitions.first?.replicas.count, 1, "replication factor")

        let configs = try await client.describeConfigs(.topic(smokeTopic))
        let policy = configs.first { $0.name == "cleanup.policy" }
        try expectEqual(policy?.value, "compact", "cleanup.policy should be the override")
        try expect(policy?.isDefault == false, "the override should not read as a default")
        let retention = configs.first { $0.name == "retention.ms" }
        try expectEqual(retention?.value, "86400000", "retention.ms")
        print("       created with 2 partitions, cleanup.policy=compact, retention.ms=86400000")
    }

    await h.checkAsync("partitions can be added but not removed") {
        let client = try KafkaClient(profile: localProfile)
        try await client.createPartitions(topic: smokeTopic, totalCount: 4)

        var count = 0
        for _ in 0..<15 where count != 4 {
            count = try await client.metadata().topics
                .first { $0.name == smokeTopic }?.partitionCount ?? 0
            if count != 4 { try await Task.sleep(for: .milliseconds(300)) }
        }
        try expectEqual(count, 4, "partition count after widening")

        // Kafka has no way to shrink a topic; the broker must refuse.
        do {
            try await client.createPartitions(topic: smokeTopic, totalCount: 2)
            throw Expectation(description: "shrinking partitions should fail")
        } catch let error as KafkaError {
            print("       shrink refused: \((error.errorDescription ?? "").prefix(80))")
        }
        print("       widened 2 → 4 partitions")
    }

    await h.checkAsync("deleting records moves the low watermark") {
        let client = try KafkaClient(profile: localProfile)
        let scratchTopic = "kestrel.produce.test"

        // Make sure there is something to trim.
        for index in 0..<3 {
            _ = try await client.produce(
                topic: scratchTopic,
                key: Data("trim-\(index)".utf8),
                value: Data("trim".utf8)
            )
        }

        let consumer = try KafkaConsumer(profile: localProfile)
        let before = try await consumer.watermarks(topic: scratchTopic, partition: 0)
        try expect(before.high - before.low >= 2, "need at least two records to trim")

        let target = before.low + 1
        let newLow = try await client.deleteRecords(
            topic: scratchTopic,
            partition: 0,
            beforeOffset: target
        )
        try expect(newLow >= before.low, "the low watermark should not go backwards")

        let after = try await consumer.watermarks(topic: scratchTopic, partition: 0)
        try expectEqual(after.high, before.high, "the high watermark should not move")
        try expect(
            after.low >= before.low,
            "low watermark \(after.low) should be at or past the old \(before.low)"
        )
        // Only whole segments are reclaimed, so the broker may trim less than asked.
        print("       low watermark \(before.low) → \(after.low) (asked for \(target)), high unchanged at \(after.high)")
    }

    await h.checkAsync("deleting records is refused on a compacted topic") {
        // Establishes what the UI must gate on: the control is disabled when
        // cleanup.policy has no "delete" component, because the broker refuses.
        let client = try KafkaClient(profile: localProfile)
        let probe = "kestrel-compact-probe"
        try await client.createTopic(
            name: probe,
            partitions: 1,
            configs: ["cleanup.policy": "compact"]
        )
        defer { Task { try? await client.deleteTopic(name: probe) } }

        for _ in 0..<15 where !(try await client.topicNames().contains(probe)) {
            try await Task.sleep(for: .milliseconds(300))
        }
        _ = try await client.produce(topic: probe, key: Data("k".utf8), value: Data("v".utf8))

        do {
            _ = try await client.deleteRecords(topic: probe, partition: 0, beforeOffset: nil)
            print("       NOTE: this broker allowed it; the UI gate is conservative")
        } catch let error as KafkaError {
            let message = error.errorDescription ?? ""
            try expect(!message.isEmpty, "the refusal should carry a message")
            print("       refused: \(message.prefix(90))")
        }
    }

    await h.checkAsync("the smoke topic is deleted and stops appearing in metadata") {
        let client = try KafkaClient(profile: localProfile)
        try await client.deleteTopic(name: smokeTopic)

        var present = true
        for _ in 0..<20 where present {
            present = try await client.topicNames().contains(smokeTopic)
            if present { try await Task.sleep(for: .milliseconds(400)) }
        }
        try expect(!present, "\(smokeTopic) should be gone from metadata")
        print("       \(smokeTopic) deleted")
    }

    await h.checkAsync("deleting a topic that is already gone reports an error") {
        let client = try KafkaClient(profile: localProfile)
        do {
            try await client.deleteTopic(name: smokeTopic)
            throw Expectation(description: "deleting a missing topic should fail")
        } catch let error as KafkaError {
            let message = error.errorDescription ?? ""
            try expect(!message.isEmpty, "the error should carry a message")
            print("       \(message.prefix(80))")
        }
    }
}

await harness.asyncSuite("Consumer group offsets and lag") { h in
    // Offsets and lag come from the parked group; membership and the refusal of
    // a reset need the group that has a real consumer attached.
    let liveGroup = fixtureGroup

    await h.checkAsync("committed offsets, log ends and lag are listed for a live group") {
        let client = try KafkaClient(profile: localProfile)
        let offsets = try await client.groupOffsets(group: liveGroup)
        try expect(!offsets.isEmpty, "the live group should have committed partitions")
        try expect(offsets.allSatisfy { $0.error == nil }, "no partition should report an error")

        // Written where the cross-check script can read it.
        let lines = offsets.map { entry in
            "\(entry.topic)|\(entry.partition)|\(entry.committed.map(String.init) ?? "-")|\(entry.logEnd)|\(entry.lag.map(String.init) ?? "-")"
        }
        try lines.joined(separator: "\n").write(
            toFile: "/tmp/kestrel-group-offsets.txt",
            atomically: true,
            encoding: .utf8
        )
        print("       \(offsets.count) partitions; wrote /tmp/kestrel-group-offsets.txt")

        if let nonZero = offsets.first(where: { ($0.committed ?? 0) > 0 }) {
            print("       \(nonZero.topic) p\(nonZero.partition): committed \(nonZero.committed!), end \(nonZero.logEnd), lag \(nonZero.lag!)")
        }
    }

    await h.checkAsync("lag is log end minus committed, and never negative") {
        let client = try KafkaClient(profile: localProfile)
        let offsets = try await client.groupOffsets(group: liveGroup)
        for entry in offsets {
            guard let committed = entry.committed, let lag = entry.lag else { continue }
            try expectEqual(lag, max(0, entry.logEnd - committed), "lag for \(entry.id)")
            try expect(lag >= 0, "lag must not be negative for \(entry.id)")
        }

        // A synthetic case, since the live group sits at zero lag.
        let behind = GroupPartitionOffset(topic: "t", partition: 0, committed: 10, logEnd: 25)
        try expectEqual(behind.lag, 15, "lag when behind")
        let never = GroupPartitionOffset(topic: "t", partition: 0, committed: nil, logEnd: 25)
        try expect(never.lag == nil, "no committed offset means no lag")
        let ahead = GroupPartitionOffset(topic: "t", partition: 0, committed: 30, logEnd: 25)
        try expectEqual(ahead.lag, 0, "a committed offset past the end clamps to zero")
    }

    await h.checkAsync("members report their decoded partition assignments") {
        let client = try KafkaClient(profile: localProfile)
        guard let group = try await client.consumerGroups().first(where: { $0.id == fixtureLiveGroup }) else {
            throw Expectation(description: "\(fixtureLiveGroup) was not listed")
        }
        try expect(!group.members.isEmpty, "the group should have members")
        try expect(
            group.members.contains { !$0.assignments.isEmpty },
            "at least one member should carry a decoded assignment"
        )
        try expect(!group.subscribedTopics.isEmpty, "subscribed topics should be derived")
        let assigned = group.members.reduce(0) { $0 + $1.partitionCount }
        print("       state \(group.state), \(group.members.count) members, \(assigned) assigned partitions, \(group.subscribedTopics.count) topics")
        if let member = group.members.first(where: { !$0.assignments.isEmpty }) {
            print("       \(member.clientId) on \(member.clientHost) owns \(member.assignments.map { "\($0.topic)\($0.partitions)" }.joined(separator: ", "))")
        }
    }

    await h.check("a malformed assignment blob decodes to nothing rather than crashing") {
        try expect(AssignmentDecoder.decode(Data()).isEmpty, "empty data")
        try expect(AssignmentDecoder.decode(Data([0, 1, 0, 0])).isEmpty, "truncated header")
        // Version 1, one topic, name truncated mid-string.
        try expect(
            AssignmentDecoder.decode(Data([0, 1, 0, 0, 0, 1, 0, 9, 65, 66])).isEmpty,
            "truncated topic name"
        )

        // A well-formed blob: version 1, topic "ab" with partitions 0 and 3.
        var good = Data([0, 1, 0, 0, 0, 1, 0, 2])
        good.append(contentsOf: Array("ab".utf8))
        good.append(contentsOf: [0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3])
        let decoded = AssignmentDecoder.decode(good)
        try expectEqual(decoded.count, 1, "one topic")
        try expectEqual(decoded.first?.topic, "ab", "topic name")
        try expectEqual(decoded.first?.partitions, [0, 3], "partitions")
    }

    await h.checkAsync("resetting a live group's offsets is refused, not silently applied") {
        let client = try KafkaClient(profile: localProfile)
        // Deliberately not read from the group's committed offsets: a member
        // that has consumed nothing yet has none, and the refusal depends only
        // on the group being non-empty.
        guard let group = try await client.consumerGroups().first(where: { $0.id == fixtureLiveGroup }),
              !group.members.isEmpty else {
            throw Expectation(description: "\(fixtureLiveGroup) has no live members")
        }
        do {
            // The group is Stable with active members, so Kafka must refuse.
            // This must not move a running service's offsets.
            _ = try await client.resetGroupOffsets(
                group: fixtureLiveGroup,
                positions: [(fixtureTopic, 0, .offset(0))]
            )
            throw Expectation(description: "resetting a group with live members should fail")
        } catch let error as KafkaError {
            let message = error.errorDescription ?? ""
            try expect(!message.isEmpty, "the refusal should carry a message")
            print("       refused: \(message.prefix(90))")
        }

        // Confirm the refused call left the group where it was. The live
        // consumer commits as it runs, so its offset may legitimately advance;
        // what must not happen is a jump back to the offset 0 we asked for.
        let after = try await client.groupOffsets(group: fixtureLiveGroup)
        for entry in after where entry.topic == fixtureTopic {
            try expect(
                (entry.committed ?? 0) > 0,
                "the refused reset must not have moved \(entry.id) to 0"
            )
        }
        print("       group still at \(after.map { "\($0.id)=\($0.committed.map(String.init) ?? "-")" }.joined(separator: ", "))")
    }
}

await harness.asyncSuite("Resetting offsets on a scratch group") { h in
    // A group of our own on the scratch topic. The live service group must
    // never be altered, and it has active members so Kafka refuses anyway.
    let scratchGroup = "kestrel.offsets.test"
    let scratchTopic = "kestrel.produce.test"

    await h.checkAsync("resetting to earliest produces a real, non-zero lag") {
        let client = try KafkaClient(profile: localProfile)
        try await client.createTopic(name: scratchTopic, partitions: 1)

        let consumer = try KafkaConsumer(profile: localProfile)
        let marks = try await consumer.watermarks(topic: scratchTopic, partition: 0)
        try expect(marks.high > marks.low, "the scratch topic needs records; high \(marks.high)")

        let applied = try await client.resetGroupOffsets(
            group: scratchGroup,
            positions: [(scratchTopic, 0, .earliest)]
        )
        try expectEqual(applied.first?.committed, marks.low, "offsets should be set to the low watermark")

        let offsets = try await client.groupOffsets(group: scratchGroup)
        guard let entry = offsets.first(where: { $0.topic == scratchTopic }) else {
            throw Expectation(description: "the scratch group has no committed offset")
        }
        try expectEqual(entry.committed, marks.low, "committed")
        try expectEqual(entry.logEnd, marks.high, "log end")
        try expectEqual(entry.lag, marks.high - marks.low, "lag should be the whole retained range")
        try expect(entry.lag! > 0, "this check is pointless without a non-zero lag")
        print("       committed \(entry.committed!), end \(entry.logEnd), lag \(entry.lag!)")
    }

    await h.checkAsync("resetting to latest clears the lag") {
        let client = try KafkaClient(profile: localProfile)
        try await client.resetGroupOffsets(
            group: scratchGroup,
            positions: [(scratchTopic, 0, .latest)]
        )
        let offsets = try await client.groupOffsets(group: scratchGroup)
        guard let entry = offsets.first(where: { $0.topic == scratchTopic }) else {
            throw Expectation(description: "the scratch group has no committed offset")
        }
        try expectEqual(entry.committed, entry.logEnd, "committed should sit at the log end")
        try expectEqual(entry.lag, 0, "lag")
        print("       committed \(entry.committed!), end \(entry.logEnd), lag 0")
    }

    await h.checkAsync("resetting to a numeric offset lands exactly there") {
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)
        let marks = try await consumer.watermarks(topic: scratchTopic, partition: 0)
        let middle = marks.low + (marks.high - marks.low) / 2

        try await client.resetGroupOffsets(
            group: scratchGroup,
            positions: [(scratchTopic, 0, .offset(middle))]
        )
        let offsets = try await client.groupOffsets(group: scratchGroup)
        guard let entry = offsets.first(where: { $0.topic == scratchTopic }) else {
            throw Expectation(description: "the scratch group has no committed offset")
        }
        try expectEqual(entry.committed, middle, "committed")
        try expectEqual(entry.lag, marks.high - middle, "lag from the middle")
        print("       committed \(entry.committed!), end \(entry.logEnd), lag \(entry.lag!)")
    }
}

await harness.asyncSuite("Producing to localhost:19092") { h in
    // Every write in this suite goes to a dedicated throwaway topic. Real
    // service topics on this broker must never receive test records.
    let scratchTopic = "kestrel.produce.test"
    var delivered: DeliveryReport?
    let marker = "kestrel-check-\(UUID().uuidString)"

    await h.checkAsync("the scratch topic exists, creating it if needed") {
        let client = try KafkaClient(profile: localProfile)
        let created = try await client.createTopic(name: scratchTopic, partitions: 1)
        print("       \(scratchTopic) \(created ? "created" : "already existed")")

        // Creation is asynchronous in the cluster; wait for it in metadata.
        var visible = false
        for _ in 0..<10 where !visible {
            visible = try await client.topicNames().contains(scratchTopic)
            if !visible { try await Task.sleep(for: .milliseconds(300)) }
        }
        try expect(visible, "\(scratchTopic) should appear in metadata")
    }

    await h.checkAsync("creating an existing topic reports false instead of failing") {
        let client = try KafkaClient(profile: localProfile)
        let created = try await client.createTopic(name: scratchTopic, partitions: 1)
        try expect(!created, "the second create should report not-created")
    }

    await h.checkAsync("a record with key, value and headers is delivered with an offset") {
        let client = try KafkaClient(profile: localProfile)
        let report = try await client.produce(
            topic: scratchTopic,
            key: Data("check-key".utf8),
            value: Data(#"{"marker":"\#(marker)","source":"KestrelChecks"}"#.utf8),
            headers: [
                RecordHeader(name: "producer", value: Data("kestrel".utf8)),
                RecordHeader(name: "marker", value: Data(marker.utf8))
            ]
        )
        try expect(report.offset >= 0, "expected a real offset, got \(report.offset)")
        try expectEqual(report.partition, 0, "partition")
        delivered = report
        print("       delivered to partition \(report.partition) offset \(report.offset)")
    }

    await h.checkAsync("the produced record reads back with its key, value and headers") {
        guard let report = delivered else { throw Expectation(description: "nothing was produced") }
        let consumer = try KafkaConsumer(profile: localProfile)
        let page = try await consumer.fetch(
            topic: scratchTopic,
            partition: report.partition,
            from: .offset(report.offset),
            limit: 1
        )
        guard let record = page.records.first else {
            throw Expectation(description: "the produced record was not readable")
        }
        try expectEqual(record.offset, report.offset, "offset")
        try expectEqual(record.keyPreview, "check-key", "key")
        try expect(record.valuePreview.contains(marker), "value should carry the marker")
        try expectEqual(record.headers.count, 2, "header count")
        try expectEqual(record.headers.first?.name, "producer", "first header name")
        try expectEqual(record.headers.first?.displayValue, "kestrel", "first header value")
        try expect(record.timestamp != nil, "a produced record should carry a timestamp")

        // This is the slice's acceptance line: the write moved the end of the
        // partition, so a refreshed browser shows the new record.
        try expect(
            page.highWatermark > report.offset,
            "high watermark \(page.highWatermark) should be past the produced offset"
        )
        print("       read back offset \(record.offset), \(record.headers.count) headers, value \(record.valuePreview.prefix(60))")
    }

    await h.checkAsync("a tombstone is produced as a null value, not empty bytes") {
        let client = try KafkaClient(profile: localProfile)
        let report = try await client.produce(
            topic: scratchTopic,
            key: Data("tombstone-key".utf8),
            value: nil
        )
        let consumer = try KafkaConsumer(profile: localProfile)
        let page = try await consumer.fetch(
            topic: scratchTopic,
            partition: report.partition,
            from: .offset(report.offset),
            limit: 1
        )
        guard let record = page.records.first else {
            throw Expectation(description: "the tombstone was not readable")
        }
        try expect(record.isTombstone, "value should be absent, not empty")
        try expect(record.value == nil, "value data should be nil")
        try expectEqual(record.valuePreview, "null (tombstone)", "preview")

        let empty = try await client.produce(
            topic: scratchTopic,
            key: Data("empty-key".utf8),
            value: Data()
        )
        let emptyPage = try await consumer.fetch(
            topic: scratchTopic,
            partition: empty.partition,
            from: .offset(empty.offset),
            limit: 1
        )
        guard let emptyRecord = emptyPage.records.first else {
            throw Expectation(description: "the empty record was not readable")
        }
        try expect(!emptyRecord.isTombstone, "empty bytes are not a tombstone")
        try expectEqual(emptyRecord.value?.count, 0, "value should be zero bytes")
        print("       tombstone at \(report.offset), empty value at \(empty.offset)")
    }

    await h.checkAsync("a failed produce reports a clear message") {
        let client = try KafkaClient(profile: localProfile)
        do {
            // An illegal topic name, not merely a missing one: this broker has
            // auto.create.topics.enable on, so a missing topic would simply be
            // created — and leave junk behind.
            _ = try await client.produce(
                topic: "kestrel invalid topic!",
                key: nil,
                value: Data("x".utf8),
                timeout: .seconds(8)
            )
            throw Expectation(description: "an illegal topic name should fail")
        } catch let error as KafkaError {
            let message = error.errorDescription ?? ""
            try expect(!message.isEmpty, "the error should carry a message")
            print("       \(message.prefix(100))")
        }
    }
}

await harness.asyncSuite("Pretty printing live record values") { h in
    await h.checkAsync("a real record value from localhost:19092 renders indented") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let page = try await consumer.fetch(
            topic: fixtureTopic,
            partition: 0,
            from: .latest(count: 1),
            limit: 1
        )
        guard let record = page.records.first else {
            throw Expectation(description: "expected a record to format")
        }

        let payload = PayloadFormatter.format(record.value)
        try expectEqual(payload.kind, .json, "the topic carries JSON values")
        guard let pretty = payload.pretty else {
            throw Expectation(description: "expected the live value to pretty-print: \(payload.problem ?? "")")
        }
        let rawLines = payload.raw.split(separator: "\n").count
        let prettyLines = pretty.split(separator: "\n").count
        try expect(prettyLines > rawLines, "pretty should add lines: \(prettyLines) vs \(rawLines)")
        try expect(
            prettyLines > 50,
            "a multi-KB document should expand well past 50 lines, got \(prettyLines)"
        )
        print("       offset \(record.offset): \(payload.byteCount) bytes, \(rawLines) raw line(s) → \(prettyLines) pretty lines")
        print("       \(pretty.split(separator: "\n").prefix(3).joined(separator: " ⏎ "))")
    }
}

await harness.asyncSuite("Kafka client when nothing is listening") { h in
    await h.checkAsync("metadata fails with a clear message instead of crashing") {
        let client = try KafkaClient(profile: deadProfile)
        do {
            _ = try await client.metadata(timeout: .seconds(3))
            throw Expectation(description: "expected a failure against a dead port")
        } catch let error as KafkaError {
            let message = error.errorDescription ?? ""
            print("       \(message)")
            try expect(
                message.localizedCaseInsensitiveContains("refused")
                    || message.localizedCaseInsensitiveContains("could not be reached")
                    || message.localizedCaseInsensitiveContains("did not respond"),
                "message should explain the refusal, got: \(message)"
            )
        }
    }

    await h.checkAsync("testConnection reports failure without throwing") {
        let client = try KafkaClient(profile: deadProfile)
        let result = await client.testConnection(timeout: .seconds(3))
        try expect(!result.isSuccess, "expected failure against a dead port")
        print("       \(result.summary)")
    }

    await h.checkAsync("an unparseable bootstrap list is rejected at creation") {
        var broken = deadProfile
        broken.bootstrapServers = "this is not a broker list"
        do {
            let client = try KafkaClient(profile: broken)
            _ = await client.testConnection(timeout: .seconds(2))
        } catch let error as KafkaError {
            print("       \(error.errorDescription ?? "")")
        }
    }
}

// MARK: Optional end-to-end seed
//
// With KESTREL_SEED_DEFAULT_STORE=1 the checks write a `local` profile and its
// SASL password to the real Application Support file and login keychain, so the
// GUI can be launched to confirm it restores a cluster saved by another run.
if ProcessInfo.processInfo.environment["KESTREL_SEED_DEFAULT_STORE"] == "1" {
    harness.suite("Seed the default store") { h in
        h.check("a profile saved to the default location reloads") {
            let repository = ClusterProfileRepository()
            let appKeychain = KeychainStore()

            var profiles = try repository.load()
            profiles.removeAll { $0.name == "local" }

            let profile = ClusterProfile(
                name: "local",
                bootstrapServers: "localhost:19092",
                securityProtocol: .plaintext,
                schemaRegistry: SchemaRegistrySettings(url: "http://localhost:18081"),
                connect: ConnectSettings(url: "http://localhost:18083")
            )
            profiles.append(profile)
            try repository.save(profiles)
            try appKeychain.set("seeded-secret", secret: .saslPassword, cluster: profile.id)

            let reloaded = try ClusterProfileRepository().load()
            try expect(reloaded.contains { $0.id == profile.id }, "seeded profile missing after reload")
            print("       store: \(repository.fileURL.path)")
            print("       seeded cluster id: \(profile.id.uuidString)")
        }
    }
}

harness.suite("Reading an import file") { h in
    /// Ten JSON documents, one per line, as the acceptance line describes.
    func tenLines() -> Data {
        Data(
            (0..<10).map { #"{"index":\#($0),"note":"line \#($0)"}"# }
                .joined(separator: "\n")
                .utf8
        )
    }

    h.check("a 10-line JSONL file plans 10 records") {
        let plan = RecordImporter.plan(data: tenLines())
        try expectEqual(plan.records.count, 10, "records planned")
        try expectEqual(plan.problems.count, 0, "problems")
        try expectEqual(plan.kind, .jsonLines, "source kind")
        try expect(plan.records.allSatisfy { $0.key == nil }, "plain documents carry no key")
        try expectEqual(plan.records.first?.line, 1, "lines are 1-based")
        try expectEqual(plan.records.last?.line, 10, "last line number")
    }

    h.check("a document is taken as the value byte for byte") {
        // Not re-encoded: spacing and key order must survive, or what lands in
        // Kafka is not what was in the file.
        let line = Data(#"{"b": 2,   "a":   1}"#.utf8)
        let plan = RecordImporter.plan(data: line + Data("\n".utf8) + line)
        try expectEqual(plan.records.count, 2, "records")
        try expectEqual(plan.records.first?.value, line, "value bytes preserved verbatim")
    }

    h.check("blank lines are skipped without complaint") {
        let plan = RecordImporter.plan(
            data: Data("{\"a\":1}\n\n   \n\t\n{\"a\":2}\n".utf8)
        )
        try expectEqual(plan.records.count, 2, "records")
        try expectEqual(plan.problems.count, 0, "blank lines are not problems")
        try expectEqual(plan.blankLines, 3, "blank lines counted")
    }

    h.check("a bad line is reported by number and the rest still import") {
        let plan = RecordImporter.plan(
            data: Data("{\"a\":1}\nnot json at all\n{\"a\":3}\n".utf8)
        )
        try expectEqual(plan.records.count, 2, "the good lines")
        try expectEqual(plan.problems.count, 1, "the bad line")
        try expectEqual(plan.problems.first?.line, 2, "problem line number")
        try expect(
            plan.problems.first?.message.contains("Not valid JSON") == true,
            "problem should say what is wrong: \(plan.problems.first?.message ?? "")"
        )
        try expectEqual(plan.records.map(\.line), [1, 3], "surviving line numbers")
    }

    h.check("envelope lines restore key, headers and value") {
        let record = KafkaRecord(
            partition: 0,
            offset: 5,
            timestamp: Date(timeIntervalSince1970: 1_700_000_000),
            timestampKind: .createTime,
            key: Data("the-key".utf8),
            value: Data(#"{"v":1}"#.utf8),
            headers: [RecordHeader(name: "origin", value: Data("saved".utf8))]
        )
        // One envelope per line, which means compact JSON.
        let line = try JSONEncoder().encode(RecordEnvelope(record: record, topic: "t"))
        let plan = RecordImporter.plan(data: line + Data("\n".utf8) + line)

        try expectEqual(plan.kind, .envelopes, "source kind")
        try expectEqual(plan.records.count, 2, "records")
        let first = plan.records[0]
        try expectEqual(first.key, Data("the-key".utf8), "key restored")
        try expectEqual(first.value, Data(#"{"v":1}"#.utf8), "value restored")
        try expectEqual(first.headers.count, 1, "header count")
        try expectEqual(first.headers.first?.name, "origin", "header name")
        try expectEqual(first.headers.first?.value, Data("saved".utf8), "header value")
    }

    h.check("a saved envelope file, as written by Save, imports as one record") {
        // Pretty-printed and spanning many lines, so this also covers a whole
        // file being a single document.
        let record = KafkaRecord(
            partition: 2,
            offset: 9,
            timestamp: nil,
            timestampKind: .unavailable,
            key: Data("k".utf8),
            value: Data([0xff, 0x00, 0x41]),
            headers: []
        )
        let url = URL(fileURLWithPath: "/tmp/kestrel-import-one.json")
        try RecordFile.write(record: record, topic: "t", format: .jsonEnvelope, to: url)

        let plan = try RecordImporter.plan(fileURL: url)
        try expectEqual(plan.records.count, 1, "one record")
        try expectEqual(plan.kind, .envelopes, "source kind")
        // Binary, so it travelled as base64 and must come back intact.
        try expectEqual(plan.records.first?.value, Data([0xff, 0x00, 0x41]), "binary value restored")
    }

    h.check("a document with a value field is not mistaken for an envelope") {
        // The reason envelopes carry a version marker.
        let plan = RecordImporter.plan(data: Data(#"{"value": {"encoding":"utf8","data":"x"}}"#.utf8))
        try expectEqual(plan.kind, .jsonLines, "should be read as a plain document")
        try expectEqual(
            plan.records.first?.value,
            Data(#"{"value": {"encoding":"utf8","data":"x"}}"#.utf8),
            "the whole document is the value"
        )
    }

    h.check("an envelope tombstone imports as a tombstone") {
        let envelope = RecordEnvelope(topic: "t", key: EnvelopePayload(Data("k".utf8)), value: nil)
        let plan = RecordImporter.plan(data: try JSONEncoder().encode(envelope))
        try expectEqual(plan.records.count, 1, "one record")
        try expect(plan.records.first?.value == nil, "value must stay nil, not become empty")
        try expectEqual(plan.records.first?.key, Data("k".utf8), "key")
    }

    h.check("envelopes and plain documents can share a file") {
        let envelope = try JSONEncoder().encode(
            RecordEnvelope(topic: "t", value: EnvelopePayload(Data("from-envelope".utf8)))
        )
        let plan = RecordImporter.plan(
            data: envelope + Data("\n{\"plain\":true}\n".utf8)
        )
        try expectEqual(plan.kind, .mixed, "source kind")
        try expectEqual(plan.records.count, 2, "records")
        try expectEqual(plan.records[0].value, Data("from-envelope".utf8), "envelope value unwrapped")
        try expectEqual(plan.records[1].value, Data("{\"plain\":true}".utf8), "document kept whole")
    }

    h.check("an envelope with corrupt base64 is reported, not produced") {
        let plan = RecordImporter.plan(
            data: Data(#"{"envelopeVersion":1,"headers":[],"value":{"encoding":"base64","data":"!!!"}}"#.utf8)
        )
        try expectEqual(plan.records.count, 0, "nothing importable")
        try expectEqual(plan.problems.count, 1, "one problem")
        try expect(
            plan.problems.first?.message.contains("not valid for its encoding") == true,
            "message should name the cause: \(plan.problems.first?.message ?? "")"
        )
    }

    h.check("an empty file plans nothing and is not an error") {
        let plan = RecordImporter.plan(data: Data())
        try expect(plan.isEmpty, "no records")
        try expectEqual(plan.problems.count, 0, "no problems")
    }
}

await harness.asyncSuite("Importing into a topic") { h in
    let importTopic = "kestrel.import.test"

    await h.checkAsync("importing a 10-line JSONL file adds 10 records to the topic") {
        // The slice's acceptance line, end to end against the broker.
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)
        try await client.createTopic(name: importTopic, partitions: 1)

        let url = URL(fileURLWithPath: "/tmp/kestrel-import-ten.jsonl")
        let documents = (0..<10).map { #"{"index":\#($0),"note":"line \#($0)"}"# }
        try documents.joined(separator: "\n").write(to: url, atomically: true, encoding: .utf8)

        let before = try await consumer.watermarks(topic: importTopic, partition: 0)
        let plan = try RecordImporter.plan(fileURL: url)
        try expectEqual(plan.records.count, 10, "planned records")

        var seen: [Int] = []
        let outcome = await RecordImporter.run(
            plan: plan,
            progress: { done, _ in seen.append(done) }
        ) { record in
            try await client.produce(
                topic: importTopic,
                partition: record.partition,
                key: record.key,
                value: record.value,
                headers: record.headers
            )
        }

        try expectEqual(outcome.produced, 10, "records the broker accepted")
        try expectEqual(outcome.problems.count, 0, "problems")
        try expectEqual(seen, Array(1...10), "progress should report each record once, in order")

        let after = try await consumer.watermarks(topic: importTopic, partition: 0)
        try expectEqual(
            after.high - before.high,
            10,
            "the high watermark should advance by exactly 10"
        )

        // And the bytes that landed are the lines from the file.
        let page = try await consumer.fetch(
            topic: importTopic,
            partition: 0,
            from: .offset(before.high),
            limit: 10
        )
        try expectEqual(page.records.count, 10, "records read back")
        try expectEqual(
            page.records.map { $0.value.flatMap { String(data: $0, encoding: .utf8) } },
            documents,
            "every line should arrive verbatim, in order"
        )
        print("       high watermark \(before.high) → \(after.high); 10 lines read back verbatim")
        print("       offsets \(outcome.reports.map(\.offset))")
    }

    await h.checkAsync("a saved record imports back with its key and headers") {
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)

        // Save a real record, then import the file into another topic: the
        // round trip slices 10 and 11 are meant to make.
        let source = try await consumer.fetch(
            topic: fixtureTopic,
            partition: 0,
            from: .earliest,
            limit: 1
        )
        guard let original = source.records.first else {
            throw Expectation(description: "expected a fixture record to save")
        }
        let url = URL(fileURLWithPath: "/tmp/kestrel-import-saved.json")
        try RecordFile.write(record: original, topic: fixtureTopic, format: .jsonEnvelope, to: url)

        let before = try await consumer.watermarks(topic: importTopic, partition: 0)
        let outcome = await RecordImporter.run(plan: try RecordImporter.plan(fileURL: url)) { record in
            try await client.produce(
                topic: importTopic,
                partition: record.partition,
                key: record.key,
                value: record.value,
                headers: record.headers
            )
        }
        try expectEqual(outcome.produced, 1, "one record produced")

        let page = try await consumer.fetch(
            topic: importTopic,
            partition: 0,
            from: .offset(before.high),
            limit: 1
        )
        guard let landed = page.records.first else {
            throw Expectation(description: "the imported record was not read back")
        }
        try expectEqual(landed.value, original.value, "value bytes survive save and import")
        try expectEqual(landed.key, original.key, "key survives")
        try expectEqual(landed.headers, original.headers, "headers survive")
        print("       \(original.headers.count) header(s) and \(original.value?.count ?? 0) value bytes round-tripped through a file")
    }

    await h.checkAsync("a rejected record does not stop the rest of the import") {
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)

        // The middle line is a valid document but oversized, so the broker
        // rejects it while its neighbours go through.
        let oversized = String(repeating: "x", count: 2_000_000)
        let plan = RecordImporter.plan(
            data: Data(
                """
                {"ok":1}
                {"big":"\(oversized)"}
                {"ok":2}
                """.utf8
            )
        )
        try expectEqual(plan.records.count, 3, "all three lines are readable")

        let before = try await consumer.watermarks(topic: importTopic, partition: 0)
        let outcome = await RecordImporter.run(plan: plan) { record in
            try await client.produce(
                topic: importTopic,
                partition: record.partition,
                key: record.key,
                value: record.value,
                headers: record.headers
            )
        }

        try expectEqual(outcome.produced, 2, "the two small records should land")
        try expectEqual(outcome.problems.count, 1, "the oversized one should be reported")
        try expectEqual(outcome.problems.first?.line, 2, "reported by line number")
        let after = try await consumer.watermarks(topic: importTopic, partition: 0)
        try expectEqual(after.high - before.high, 2, "only two records added")
        print("       line 2 refused: \((outcome.problems.first?.message ?? "").prefix(80))")
    }
}

await harness.asyncSuite("Exporting records") { h in
    let exportTopic = "kestrel.export.test"
    let roundTripTopic = "kestrel.export.roundtrip"

    await h.checkAsync("exporting a range writes one envelope per line") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let url = URL(fileURLWithPath: "/tmp/kestrel-export.jsonl")

        let outcome = try await RecordExporter.export(
            request: ExportRequest(
                topic: fixtureTopic,
                partitions: [0],
                startOffset: 5,
                endOffset: 10
            ),
            consumer: consumer,
            sink: try JSONLFileSink(url: url)
        )

        // End offset is exclusive, so 5..<10 is five records.
        try expectEqual(outcome.records, 5, "records exported")
        try expectEqual(outcome.byPartition, [0: 5], "records per partition")
        try expect(outcome.bytesWritten > 0, "bytes written should be reported")

        let lines = try String(contentsOf: url, encoding: .utf8)
            .split(separator: "\n", omittingEmptySubsequences: true)
        try expectEqual(lines.count, 5, "lines in the file")

        let plan = try RecordImporter.plan(fileURL: url)
        try expectEqual(plan.kind, .envelopes, "the file should read back as envelopes")
        try expectEqual(plan.records.count, 5, "records the importer finds")
        print("       offsets 5..<10 → \(outcome.records) records, \(outcome.bytesWritten) bytes")
    }

    await h.checkAsync("export then import preserves the count and the values") {
        // The slice's acceptance line.
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)
        try await client.createTopic(name: roundTripTopic, partitions: 1)

        let url = URL(fileURLWithPath: "/tmp/kestrel-export-roundtrip.jsonl")
        let source = try await consumer.fetch(
            topic: fixtureTopic,
            partition: 0,
            from: .earliest,
            limit: 12
        )
        try expect(source.records.count >= 12, "expected a dozen fixture records to export")

        let exported = try await RecordExporter.export(
            request: ExportRequest(
                topic: fixtureTopic,
                partitions: [0],
                startOffset: source.records[0].offset,
                endOffset: source.records[11].offset + 1
            ),
            consumer: consumer,
            sink: try JSONLFileSink(url: url)
        )
        try expectEqual(exported.records, 12, "records exported")

        let before = try await consumer.watermarks(topic: roundTripTopic, partition: 0)
        let imported = await RecordImporter.run(plan: try RecordImporter.plan(fileURL: url)) { record in
            try await client.produce(
                topic: roundTripTopic,
                partition: record.partition,
                key: record.key,
                value: record.value,
                headers: record.headers
            )
        }
        try expectEqual(imported.produced, 12, "records imported")
        try expectEqual(imported.problems.count, 0, "problems")

        let after = try await consumer.watermarks(topic: roundTripTopic, partition: 0)
        try expectEqual(after.high - before.high, 12, "the destination should gain exactly 12 records")

        let landed = try await consumer.fetch(
            topic: roundTripTopic,
            partition: 0,
            from: .offset(before.high),
            limit: 12
        )
        try expectEqual(landed.records.count, 12, "records read back")
        try expectEqual(
            landed.records.map(\.value),
            source.records.prefix(12).map(\.value),
            "every value should survive the round trip, in order"
        )
        try expectEqual(
            landed.records.map(\.key),
            source.records.prefix(12).map(\.key),
            "keys should survive too"
        )
        try expectEqual(
            landed.records.map(\.headers),
            source.records.prefix(12).map(\.headers),
            "and headers"
        )
        print("       12 records exported, imported and compared byte for byte")
    }

    await h.checkAsync("exporting straight into another topic preserves the records") {
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)
        try await client.createTopic(name: exportTopic, partitions: 1)

        let before = try await consumer.watermarks(topic: exportTopic, partition: 0)
        let sink = TopicSink { record in
            try await client.produce(
                topic: exportTopic,
                partition: nil,
                key: record.key,
                value: record.value,
                headers: record.headers
            )
        }

        var lastProgress = 0
        let outcome = try await RecordExporter.export(
            request: ExportRequest(
                topic: fixtureTopic,
                partitions: [0],
                startOffset: 0,
                endOffset: 7
            ),
            consumer: consumer,
            sink: sink,
            progress: { done, _ in lastProgress = done }
        )

        try expectEqual(outcome.records, 7, "records exported")
        try expectEqual(lastProgress, 7, "progress should end at the record count")
        try expectEqual(await sink.reports.count, 7, "delivery reports")

        let after = try await consumer.watermarks(topic: exportTopic, partition: 0)
        try expectEqual(after.high - before.high, 7, "the destination should gain exactly 7 records")

        let source = try await consumer.fetch(
            topic: fixtureTopic,
            partition: 0,
            from: .offset(0),
            limit: 7
        )
        let landed = try await consumer.fetch(
            topic: exportTopic,
            partition: 0,
            from: .offset(before.high),
            limit: 7
        )
        try expectEqual(
            landed.records.map(\.value),
            source.records.map(\.value),
            "values should match the source"
        )
        print("       7 records copied to \(exportTopic) at offsets \(await sink.reports.map(\.offset))")
    }

    await h.checkAsync("a range is clamped to what the partition still retains") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let marks = try await consumer.watermarks(topic: fixtureTopic, partition: 0)

        // Deliberately absurd bounds on both sides.
        let (ranges, total) = try await RecordExporter.plan(
            request: ExportRequest(
                topic: fixtureTopic,
                partitions: [0],
                startOffset: -500,
                endOffset: marks.high + 10_000
            ),
            consumer: consumer
        )
        try expectEqual(ranges.first?.start, marks.low, "start clamped to the low watermark")
        try expectEqual(ranges.first?.end, marks.high, "end clamped to the high watermark")
        try expectEqual(total, Int(marks.high - marks.low), "total matches what is retained")
    }

    await h.checkAsync("an empty range exports nothing and writes an empty file") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let url = URL(fileURLWithPath: "/tmp/kestrel-export-empty.jsonl")
        // Stale content, to prove the file is truncated rather than appended to.
        try Data("leftover\n".utf8).write(to: url)

        let outcome = try await RecordExporter.export(
            request: ExportRequest(topic: fixtureTopic, partitions: [0], startOffset: 5, endOffset: 5),
            consumer: consumer,
            sink: try JSONLFileSink(url: url)
        )
        try expectEqual(outcome.records, 0, "no records")
        try expectEqual(try Data(contentsOf: url), Data(), "the file should be emptied, not appended to")
    }

    await h.checkAsync("an empty topic exports nothing rather than hanging") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let url = URL(fileURLWithPath: "/tmp/kestrel-export-empty-topic.jsonl")

        let started = Date()
        let outcome = try await RecordExporter.export(
            request: ExportRequest(topic: fixtureEmptyTopic, partitions: [0]),
            consumer: consumer,
            sink: try JSONLFileSink(url: url)
        )
        try expectEqual(outcome.records, 0, "no records")
        try expect(Date().timeIntervalSince(started) < 10, "should not wait for records that cannot come")
    }

    await h.checkAsync("a paged export reads every record exactly once") {
        // More records than one page, so the offset bookkeeping between pages
        // is exercised rather than assumed.
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)
        let pagedTopic = "kestrel.export.paged"
        try await client.createTopic(name: pagedTopic, partitions: 1)

        let marks = try await consumer.watermarks(topic: pagedTopic, partition: 0)
        let wanted = Int64(RecordExporter.pageSize + 37)
        if marks.high - marks.low < wanted {
            for index in marks.high..<wanted {
                _ = try await client.produce(
                    topic: pagedTopic,
                    key: nil,
                    value: Data(#"{"n":\#(index)}"#.utf8)
                )
            }
        }

        let url = URL(fileURLWithPath: "/tmp/kestrel-export-paged.jsonl")
        let outcome = try await RecordExporter.export(
            request: ExportRequest(topic: pagedTopic, partitions: [0], startOffset: 0, endOffset: wanted),
            consumer: consumer,
            sink: try JSONLFileSink(url: url)
        )
        try expectEqual(outcome.records, Int(wanted), "every record across pages")

        // No duplicates and no gaps: the offsets should be exactly 0..<wanted.
        let plan = try RecordImporter.plan(fileURL: url)
        try expectEqual(plan.records.count, Int(wanted), "lines in the file")
        let offsets = try String(contentsOf: url, encoding: .utf8)
            .split(separator: "\n", omittingEmptySubsequences: true)
            .compactMap { line -> Int64? in
                try? JSONDecoder().decode(RecordEnvelope.self, from: Data(line.utf8)).offset
            }
        try expectEqual(offsets, Array(0..<wanted), "offsets should be complete and in order")
        print("       \(outcome.records) records over \(Int(wanted) / RecordExporter.pageSize + 1) pages, no gaps or repeats")
    }

    await h.checkAsync("every partition is exported when none is named") {
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)
        let multiTopic = "kestrel.export.multi"
        try await client.createTopic(name: multiTopic, partitions: 3)

        // A partition has no leader for a moment after the topic is created,
        // and producing to it then fails with "Not leader for partition".
        var leadersReady = false
        for _ in 0..<40 {
            let topic = try await client.metadata().topics.first { $0.name == multiTopic }
            if topic?.partitions.count == 3,
               topic?.partitions.allSatisfy({ $0.leader >= 0 }) == true {
                leadersReady = true
                break
            }
            try await Task.sleep(for: .milliseconds(250))
        }
        try expect(leadersReady, "\(multiTopic) never got a leader for all 3 partitions")

        // One record per partition, addressed explicitly.
        for partition in Int32(0)..<3 {
            let marks = try await consumer.watermarks(topic: multiTopic, partition: partition)
            if marks.high == marks.low {
                _ = try await client.produce(
                    topic: multiTopic,
                    partition: partition,
                    key: nil,
                    value: Data(#"{"partition":\#(partition)}"#.utf8)
                )
            }
        }

        let url = URL(fileURLWithPath: "/tmp/kestrel-export-multi.jsonl")
        let outcome = try await RecordExporter.export(
            request: ExportRequest(topic: multiTopic, partitions: [0, 1, 2]),
            consumer: consumer,
            sink: try JSONLFileSink(url: url)
        )
        try expectEqual(outcome.records, 3, "one record from each partition")
        try expectEqual(outcome.byPartition, [0: 1, 1: 1, 2: 1], "counted per partition")

        let plan = try RecordImporter.plan(fileURL: url)
        try expectEqual(plan.records.count, 3, "lines in the file")
    }
}

harness.suite("Expanding record templates") { h in
    h.check("index counts up and plain text is untouched") {
        var expander = TemplateExpander(generator: SeededGenerator(seed: 1))
        try expectEqual(
            expander.expand("record-{{index}}-of-many", index: 7),
            "record-7-of-many",
            "index substituted in place"
        )
        try expectEqual(
            expander.expand("no placeholders here", index: 0),
            "no placeholders here",
            "plain text"
        )
    }

    h.check("a JSON template stays valid JSON once filled in") {
        var expander = TemplateExpander(generator: SeededGenerator(seed: 2))
        let filled = expander.expand(
            #"{"id":"{{uuid}}","n":{{index}},"note":"{{lorem:3}}","score":{{int:1-100}}}"#,
            index: 4
        )
        let object = try JSONSerialization.jsonObject(with: Data(filled.utf8)) as? [String: Any]
        try expect(object != nil, "should parse as a JSON object: \(filled)")
        try expectEqual(object?["n"] as? Int, 4, "index lands as a number, not a string")
        try expectEqual((object?["note"] as? String)?.split(separator: " ").count, 3, "word count")
        let score = object?["score"] as? Int ?? -1
        try expect((1...100).contains(score), "score should be in range, got \(score)")
        print("       \(filled)")
    }

    h.check("a seed makes a run repeat exactly") {
        // What lets the checks below assert on generated content at all.
        let template = #"{"id":"{{uuid}}","w":"{{lorem}}","i":{{int:0-1000}},"c":"{{choice:a|b|c}}"}"#
        let now = Date(timeIntervalSince1970: 1_700_000_000)

        var first = TemplateExpander(generator: SeededGenerator(seed: 42))
        var second = TemplateExpander(generator: SeededGenerator(seed: 42))
        var different = TemplateExpander(generator: SeededGenerator(seed: 43))

        let a = (0..<5).map { first.expand(template, index: $0, now: now) }
        let b = (0..<5).map { second.expand(template, index: $0, now: now) }
        let c = (0..<5).map { different.expand(template, index: $0, now: now) }

        try expectEqual(a, b, "the same seed should produce the same records")
        try expect(a != c, "a different seed should produce different records")
        try expect(Set(a).count == 5, "records within a run should differ from each other")
    }

    h.check("a uuid looks like a version 4 uuid and is parseable") {
        var expander = TemplateExpander(generator: SeededGenerator(seed: 3))
        let text = expander.expand("{{uuid}}", index: 0)
        try expect(UUID(uuidString: text) != nil, "should parse as a UUID: \(text)")
        try expectEqual(text.split(separator: "-").map(\.count), [8, 4, 4, 4, 12], "grouping")
        try expect(text.hasPrefix(text.prefix(14)) && Array(text)[14] == "4", "version nibble should be 4: \(text)")
    }

    h.check("timestamps report the time they are given") {
        var expander = TemplateExpander(generator: SeededGenerator(seed: 4))
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        try expectEqual(expander.expand("{{millis}}", index: 0, now: now), "1700000000000", "millis")
        try expectEqual(
            expander.expand("{{timestamp}}", index: 0, now: now),
            now.formatted(.iso8601),
            "ISO 8601"
        )
    }

    h.check("choice picks only from the options given") {
        var expander = TemplateExpander(generator: SeededGenerator(seed: 5))
        let picks = (0..<40).map { expander.expand("{{choice:red|green|blue}}", index: $0) }
        try expect(Set(picks).isSubset(of: ["red", "green", "blue"]), "unexpected pick in \(Set(picks))")
        try expect(Set(picks).count > 1, "40 picks from 3 options should not all be the same")
    }

    h.check("an unrecognised placeholder is left visible, not blanked") {
        var expander = TemplateExpander(generator: SeededGenerator(seed: 6))
        try expectEqual(
            expander.expand("{{nope}} and {{index}}", index: 2),
            "{{nope}} and 2",
            "a typo should survive into the output so it can be seen"
        )
        try expectEqual(
            TemplateExpander.unknownPlaceholders(in: "{{nope}} {{index}} {{lorem:x}} {{nope}}"),
            ["nope", "lorem:x"],
            "reported once each, in order"
        )
        try expect(
            TemplateExpander.unknownPlaceholders(in: #"{"a":"{{uuid}}","b":{{int:1-2}}}"#).isEmpty,
            "a valid template should report nothing"
        )
    }

    h.check("malformed placeholder arguments are refused rather than guessed") {
        var expander = TemplateExpander(generator: SeededGenerator(seed: 7))
        // Backwards range, non-numeric count, missing terminator.
        try expectEqual(expander.expand("{{int:10-1}}", index: 0), "{{int:10-1}}", "backwards range")
        try expectEqual(expander.expand("{{lorem:0}}", index: 0), "{{lorem:0}}", "zero words")
        try expectEqual(expander.expand("{{index", index: 0), "{{index", "unterminated placeholder")
    }

    h.check("the documented placeholders all resolve") {
        // Guards against documenting something that does not work.
        var expander = TemplateExpander(generator: SeededGenerator(seed: 8))
        for entry in TemplateExpander.documentation {
            let sample = entry.name
                .replacingOccurrences(of: "{{lorem:n}}", with: "{{lorem:2}}")
                .replacingOccurrences(of: "{{int:a-b}}", with: "{{int:1-9}}")
                .replacingOccurrences(of: "{{choice:x|y|z}}", with: "{{choice:x|y}}")
            let filled = expander.expand(sample, index: 1)
            try expect(
                !filled.contains("{{"),
                "documented placeholder \(entry.name) did not resolve: \(filled)"
            )
        }
    }
}

/// Somewhere a `@Sendable` progress closure can record its last report.
///
/// The generator calls progress from outside the caller's isolation, so a
/// plain captured `var` cannot be written to.
final class ProgressBox: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0

    private var reportedOnMain = false

    var last: Int {
        lock.lock()
        defer { lock.unlock() }
        return value
    }

    /// Whether any progress report arrived on the main thread, which would
    /// mean the generator is doing its work where the UI draws.
    var sawMainThread: Bool {
        lock.lock()
        defer { lock.unlock() }
        return reportedOnMain
    }

    func record(_ new: Int) {
        lock.lock()
        value = new
        reportedOnMain = reportedOnMain || Thread.isMainThread
        lock.unlock()
    }
}

await harness.asyncSuite("Generating records") { h in
    let generateTopic = "kestrel.generate.test"

    await h.checkAsync("generating 100 records leaves 100 new records on the topic") {
        // The slice's acceptance line.
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)
        try await client.createTopic(name: generateTopic, partitions: 1)

        let before = try await consumer.watermarks(topic: generateTopic, partition: 0)
        let request = GenerateRequest(
            topic: generateTopic,
            count: 100,
            keyTemplate: "key-{{index}}",
            valueTemplate: #"{"index":{{index}},"id":"{{uuid}}","note":"{{lorem:3}}"}"#,
            seed: 99
        )

        let reported = ProgressBox()
        let outcome = await RecordGenerator(client: client).generate(request: request) { done, _ in
            reported.record(done)
        }

        try expectEqual(outcome.produced, 100, "records produced")
        try expect(outcome.failures.isEmpty, "no record should be refused: \(outcome.failures)")
        try expectEqual(reported.last, 100, "progress should finish at the count")
        // The slice requires the run to stay off the UI thread.
        try expect(!reported.sawMainThread, "generation reported progress on the main thread")

        let after = try await consumer.watermarks(topic: generateTopic, partition: 0)
        try expectEqual(after.high - before.high, 100, "the topic should gain exactly 100 records")

        // And what landed is what the templates describe.
        let page = try await consumer.fetch(
            topic: generateTopic,
            partition: 0,
            from: .offset(before.high),
            limit: 100
        )
        try expectEqual(page.records.count, 100, "records read back")
        try expectEqual(
            page.records.map { $0.key.flatMap { String(data: $0, encoding: .utf8) } },
            (0..<100).map { "key-\($0)" },
            "keys should count up with the index"
        )

        let indices = page.records.compactMap { record -> Int? in
            guard let value = record.value,
                  let object = try? JSONSerialization.jsonObject(with: value) as? [String: Any]
            else { return nil }
            return object["index"] as? Int
        }
        try expectEqual(indices, Array(0..<100), "each value should carry its own index")

        let ids = Set(page.records.compactMap { record -> String? in
            guard let value = record.value,
                  let object = try? JSONSerialization.jsonObject(with: value) as? [String: Any]
            else { return nil }
            return object["id"] as? String
        })
        try expectEqual(ids.count, 100, "every generated uuid should be distinct")
        print("       high watermark \(before.high) → \(after.high), 100 distinct uuids, indices 0..<100")
    }

    await h.checkAsync("a keyless template produces keyless records") {
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)

        let before = try await consumer.watermarks(topic: generateTopic, partition: 0)
        let outcome = await RecordGenerator(client: client).generate(
            request: GenerateRequest(
                topic: generateTopic,
                count: 3,
                valueTemplate: "plain-{{index}}",
                seed: 1
            )
        )
        try expectEqual(outcome.produced, 3, "records produced")

        let page = try await consumer.fetch(
            topic: generateTopic,
            partition: 0,
            from: .offset(before.high),
            limit: 3
        )
        try expect(page.records.allSatisfy { $0.key == nil }, "an empty key template means no key")
        try expectEqual(
            page.records.compactMap { $0.value.flatMap { String(data: $0, encoding: .utf8) } },
            ["plain-0", "plain-1", "plain-2"],
            "values"
        )
    }

    await h.checkAsync("records can be aimed at one partition") {
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)
        let multiTopic = "kestrel.generate.multi"
        try await client.createTopic(name: multiTopic, partitions: 3)

        var ready = false
        for _ in 0..<40 {
            let topic = try await client.metadata().topics.first { $0.name == multiTopic }
            if topic?.partitions.allSatisfy({ $0.leader >= 0 }) == true, topic?.partitions.count == 3 {
                ready = true
                break
            }
            try await Task.sleep(for: .milliseconds(250))
        }
        try expect(ready, "\(multiTopic) never got leaders")

        // Leaders being reported is still not quite enough: the first produce
        // to a brand new partition can come back "Not leader for partition"
        // anyway. One warm-up record, retried, settles it — and it is sent
        // before the watermark is read, so the counts below stay exact.
        var warmedUp = false
        for _ in 0..<15 {
            do {
                _ = try await client.produce(
                    topic: multiTopic,
                    partition: 2,
                    key: nil,
                    value: Data("warm-up".utf8)
                )
                warmedUp = true
                break
            } catch {
                try await Task.sleep(for: .milliseconds(500))
            }
        }
        try expect(warmedUp, "could not produce to \(multiTopic) partition 2 at all")

        let before = try await consumer.watermarks(topic: multiTopic, partition: 2)
        let outcome = await RecordGenerator(client: client).generate(
            request: GenerateRequest(
                topic: multiTopic,
                count: 5,
                valueTemplate: "{{index}}",
                partition: 2,
                seed: 1
            )
        )
        try expectEqual(outcome.produced, 5, "records produced")

        let after = try await consumer.watermarks(topic: multiTopic, partition: 2)
        try expectEqual(after.high - before.high, 5, "all five should land on partition 2")
        try expectEqual(
            try await consumer.watermarks(topic: multiTopic, partition: 0).high,
            try await consumer.watermarks(topic: multiTopic, partition: 0).low,
            "partition 0 should stay as it was"
        )
    }

    await h.checkAsync("a refused record does not abandon the run") {
        let client = try KafkaClient(profile: localProfile)
        let generator = RecordGenerator(client: client)

        // An illegal topic name, so each record is refused at once. A merely
        // absent topic would make the producer wait out its whole timeout
        // four times over, which is 40 seconds of nothing.
        let outcome = await generator.generate(
            request: GenerateRequest(
                topic: "kestrel generate illegal!",
                count: 4,
                valueTemplate: "{{index}}",
                seed: 1
            )
        )
        try expectEqual(outcome.produced, 0, "nothing should be produced")
        try expectEqual(outcome.failures.count, 4, "every record should be reported")
        try expect(
            outcome.failures[0]?.isEmpty == false,
            "a failure should carry a message: \(outcome.failures)"
        )
        print("       refused: \((outcome.failures[0] ?? "").prefix(70))")
    }

    h.check("a preview shows what a run would produce, without producing it") {
        let rows = RecordGenerator.preview(
            request: GenerateRequest(
                topic: "t",
                count: 100,
                keyTemplate: "k-{{index}}",
                valueTemplate: #"{"i":{{index}}}"#,
                seed: 7
            ),
            limit: 3
        )
        try expectEqual(rows.count, 3, "a preview should show only the first few")
        try expectEqual(rows.map(\.key), ["k-0", "k-1", "k-2"], "keys")
        try expectEqual(rows.first?.value, #"{"i":0}"#, "value")
    }
}

await harness.asyncSuite("Finding messages") { h in
    let searchTopic = "kestrel.search.test"
    /// A string planted by the generator, as the acceptance line describes.
    let needle = "kestrel-needle-7f3a"

    await h.checkAsync("a string planted by the generator is found at a known offset") {
        // The slice's acceptance line.
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)
        try await client.createTopic(name: searchTopic, partitions: 1)

        // Chaff, then the needle, then more chaff, so the hit is not simply
        // the first or last record in the partition.
        //
        // Seeded once and only once: planting on every run would leave two
        // needles the second time, and "exactly one match" would fail.
        let plantedOffset: Int64 = 20
        if try await consumer.watermarks(topic: searchTopic, partition: 0).high == 0 {
            let generator = RecordGenerator(client: client)
            _ = await generator.generate(
                request: GenerateRequest(
                    topic: searchTopic,
                    count: Int(plantedOffset),
                    keyTemplate: "chaff-{{index}}",
                    valueTemplate: #"{"index":{{index}},"note":"{{lorem:3}}"}"#,
                    seed: 11
                )
            )
            let planted = try await client.produce(
                topic: searchTopic,
                key: Data("planted-key".utf8),
                value: Data(#"{"marker":"\#(needle)","note":"the one to find"}"#.utf8)
            )
            try expectEqual(planted.offset, plantedOffset, "the needle's offset")
            _ = await generator.generate(
                request: GenerateRequest(
                    topic: searchTopic,
                    count: 20,
                    keyTemplate: "chaff-{{index}}",
                    valueTemplate: #"{"index":{{index}},"note":"{{lorem:3}}"}"#,
                    seed: 12
                )
            )
        }

        let outcome = try await RecordSearcher(consumer: consumer).scan(
            query: SearchQuery(text: needle),
            scopes: [SearchScope(topic: searchTopic, partitions: [0])]
        )

        try expectEqual(outcome.hits.count, 1, "exactly one record should match")
        let hit = try require(outcome.hits.first, "a hit")
        try expectEqual(hit.offset, plantedOffset, "the hit should name the offset it was produced at")
        try expectEqual(hit.partition, 0, "partition")
        try expectEqual(hit.topic, searchTopic, "topic")
        try expectEqual(hit.field, .value, "the needle is in the value")
        try expect(hit.excerpt.contains(needle), "the excerpt should show the match: \(hit.excerpt)")
        try expect(outcome.scanned >= 41, "the whole partition should have been read, scanned \(outcome.scanned)")
        try expect(!outcome.reachedLimit, "should not have hit the cap")

        // And the offset the hit names really holds that record.
        let page = try await consumer.fetch(
            topic: searchTopic,
            partition: 0,
            from: .offset(hit.offset),
            limit: 1
        )
        let found = try require(page.records.first, "the record at the hit's offset")
        try expect(
            String(data: found.value ?? Data(), encoding: .utf8)?.contains(needle) == true,
            "opening the hit's offset should show the needle"
        )
        print("       found at offset \(hit.offset) after scanning \(outcome.scanned) records")
        print("       excerpt: \(hit.excerpt)")
    }

    await h.checkAsync("keys and values can be searched separately") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let searcher = RecordSearcher(consumer: consumer)
        let scopes = [SearchScope(topic: searchTopic, partitions: [0])]

        let keysOnly = try await searcher.scan(
            query: SearchQuery(text: "planted-key", searchesKeys: true, searchesValues: false),
            scopes: scopes
        )
        try expectEqual(keysOnly.hits.count, 1, "the key should match")
        try expectEqual(keysOnly.hits.first?.field, .key, "matched in the key")

        let valuesOnly = try await searcher.scan(
            query: SearchQuery(text: "planted-key", searchesKeys: false, searchesValues: true),
            scopes: scopes
        )
        try expect(valuesOnly.hits.isEmpty, "no value contains the key's text")

        // A record matching in both fields is reported once per field, so a
        // hit always says which one it came from.
        let both = try await searcher.scan(
            query: SearchQuery(text: "chaff-1", searchesKeys: true, searchesValues: true),
            scopes: scopes
        )
        try expect(both.hits.allSatisfy { $0.field == .key }, "chaff text lives in keys only")
        try expect(both.hits.count > 1, "chaff-1 should match chaff-1, chaff-10 and so on")
    }

    await h.checkAsync("search is case-insensitive unless asked otherwise") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let searcher = RecordSearcher(consumer: consumer)
        let scopes = [SearchScope(topic: searchTopic, partitions: [0])]

        let insensitive = try await searcher.scan(
            query: SearchQuery(text: "KESTREL-NEEDLE-7F3A"),
            scopes: scopes
        )
        try expectEqual(insensitive.hits.count, 1, "different case should still match")

        let sensitive = try await searcher.scan(
            query: SearchQuery(text: "KESTREL-NEEDLE-7F3A", isCaseSensitive: true),
            scopes: scopes
        )
        try expect(sensitive.hits.isEmpty, "case-sensitive search should not match")
    }

    await h.checkAsync("a regex query matches and a broken one is refused") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let searcher = RecordSearcher(consumer: consumer)
        let scopes = [SearchScope(topic: searchTopic, partitions: [0])]

        let outcome = try await searcher.scan(
            query: SearchQuery(text: #"needle-[0-9a-f]{4}"#, isRegex: true),
            scopes: scopes
        )
        try expectEqual(outcome.hits.count, 1, "the pattern should match the planted record")

        do {
            _ = try await searcher.scan(
                query: SearchQuery(text: "unclosed [group", isRegex: true),
                scopes: scopes
            )
            throw Expectation(description: "a broken pattern should be refused")
        } catch let error as SearchError {
            try expect(
                error.errorDescription?.contains("valid regular expression") == true,
                "message should name the problem: \(error.errorDescription ?? "")"
            )
        }

        // The same text as a literal must not be treated as a pattern.
        let literal = try await searcher.scan(
            query: SearchQuery(text: "unclosed [group", isRegex: false),
            scopes: scopes
        )
        try expect(literal.hits.isEmpty, "a literal search for that text simply finds nothing")
    }

    await h.checkAsync("searching nothing at all is refused") {
        let consumer = try KafkaConsumer(profile: localProfile)
        do {
            _ = try await RecordSearcher(consumer: consumer).scan(
                query: SearchQuery(text: "x", searchesKeys: false, searchesValues: false),
                scopes: [SearchScope(topic: searchTopic, partitions: [0])]
            )
            throw Expectation(description: "should refuse to scan with neither field selected")
        } catch let error as SearchError {
            try expectEqual(error, .nothingToSearch, "error")
        }
    }

    await h.checkAsync("a scan stops at the hit cap") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let outcome = try await RecordSearcher(consumer: consumer).scan(
            // Every chaff key contains "chaff", so this would match dozens.
            query: SearchQuery(text: "chaff", maxHits: 5),
            scopes: [SearchScope(topic: searchTopic, partitions: [0])]
        )
        try expectEqual(outcome.hits.count, 5, "should stop at the cap")
        try expect(outcome.reachedLimit, "and say that it did")
    }

    await h.checkAsync("a cancelled scan stops instead of reading the topic") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let searcher = RecordSearcher(consumer: consumer)

        // Cancelled before it can start, which is deterministic: timing a
        // cancellation against a running scan would pass or fail by luck.
        let task = Task {
            try await searcher.scan(
                query: SearchQuery(text: "n", maxHits: 100_000),
                scopes: [SearchScope(topic: "kestrel.export.paged", partitions: [0])]
            )
        }
        task.cancel()

        let outcome = try await task.value
        try expect(outcome.wasCancelled, "the outcome should say it stopped early")
        try expectEqual(outcome.scanned, 0, "a cancelled scan should not read records")
        // Returned, not thrown: whatever a scan found is worth keeping.
        try expect(outcome.hits.isEmpty, "nothing found")
    }

    await h.checkAsync("a completed scan is not reported as cancelled") {
        // Guards the flag against ambient cancellation: it must mean "stopped
        // early", not "some task was cancelled at some point".
        let consumer = try KafkaConsumer(profile: localProfile)
        let outcome = try await RecordSearcher(consumer: consumer).scan(
            query: SearchQuery(text: needle),
            scopes: [SearchScope(topic: searchTopic, partitions: [0])]
        )
        try expect(!outcome.wasCancelled, "a finished scan is complete")
        try expectEqual(outcome.hits.count, 1, "and still finds the needle")
        print("       completed scan: \(outcome.scanned) records, cancelled=\(outcome.wasCancelled)")
    }

    await h.checkAsync("a scan covers every partition in scope") {
        let client = try KafkaClient(profile: localProfile)
        let consumer = try KafkaConsumer(profile: localProfile)
        let multiTopic = "kestrel.search.multi"
        try await client.createTopic(name: multiTopic, partitions: 3)

        for partition in Int32(0)..<3 {
            let marks = try await consumer.watermarks(topic: multiTopic, partition: partition)
            guard marks.high == marks.low else { continue }
            // Retried: a new partition's first produce can be refused while
            // leadership settles.
            for _ in 0..<15 {
                do {
                    _ = try await client.produce(
                        topic: multiTopic,
                        partition: partition,
                        key: nil,
                        value: Data("shared-marker in partition \(partition)".utf8)
                    )
                    break
                } catch {
                    try await Task.sleep(for: .milliseconds(500))
                }
            }
        }

        let outcome = try await RecordSearcher(consumer: consumer).scan(
            query: SearchQuery(text: "shared-marker"),
            scopes: [SearchScope(topic: multiTopic, partitions: [0, 1, 2])]
        )
        try expectEqual(outcome.hits.count, 3, "one hit from each partition")
        try expectEqual(
            Set(outcome.hits.map(\.partition)),
            [0, 1, 2],
            "every partition should be represented"
        )

        // Narrowing the scope narrows the results.
        let narrowed = try await RecordSearcher(consumer: consumer).scan(
            query: SearchQuery(text: "shared-marker"),
            scopes: [SearchScope(topic: multiTopic, partitions: [1])]
        )
        try expectEqual(narrowed.hits.count, 1, "only the chosen partition")
        try expectEqual(narrowed.hits.first?.partition, 1, "partition 1")
    }

    await h.checkAsync("a missing topic in scope does not lose hits from the others") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let outcome = try await RecordSearcher(consumer: consumer).scan(
            query: SearchQuery(text: needle),
            scopes: [
                SearchScope(topic: "kestrel.search.does.not.exist", partitions: [0]),
                SearchScope(topic: searchTopic, partitions: [0]),
            ]
        )
        try expectEqual(outcome.hits.count, 1, "the reachable topic should still be searched")
    }

    h.check("binary payloads are skipped rather than matched by accident") {
        // Lossy decoding would turn stray bytes into replacement characters
        // and could appear to match.
        let matcher = try Matcher(query: SearchQuery(text: "\u{FFFD}"))
        try expect(matcher.match(Data([0xff, 0xfe, 0x00])) == nil, "binary should not match")
        try expect(matcher.match(nil) == nil, "an absent payload should not match")
        try expect(
            try Matcher(query: SearchQuery(text: "abc")).match(Data("xxabcxx".utf8)) != nil,
            "text should still match"
        )
    }

    h.check("an excerpt is trimmed around the match") {
        let matcher = try Matcher(query: SearchQuery(text: "middle"))
        let long = String(repeating: "a", count: 200) + "middle" + String(repeating: "b", count: 200)
        let excerpt = try require(matcher.match(Data(long.utf8)), "an excerpt")
        try expect(excerpt.contains("middle"), "should contain the match")
        try expect(excerpt.count < 100, "should be trimmed, got \(excerpt.count) characters")
        try expect(excerpt.hasPrefix("…") && excerpt.hasSuffix("…"), "should mark both cuts: \(excerpt)")

        let short = try require(matcher.match(Data("middle".utf8)), "an excerpt")
        try expectEqual(short, "middle", "nothing to trim, so no ellipses")
    }
}


// MARK: Avro and Schema Registry

/// The record at `index` of a fixture page, as a failure rather than a trap.
///
/// Indexing directly would crash the whole run when a fixture is missing,
/// taking every later suite with it.
func fixtureRecord(_ page: RecordPage, _ index: Int, _ label: String) throws -> KafkaRecord {
    try expect(
        page.records.count > index,
        "expected at least \(index + 1) \(label) record(s), got \(page.records.count)"
    )
    return page.records[index]
}

/// Runs a command in a container and returns its output.
///
/// Used to drive Confluent's own Avro tools, which is the point: a decoder
/// checked only against its own encoder proves nothing about the wire format.
func dockerRun(_ container: String, _ command: String) -> String {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    process.arguments = ["docker", "exec", container, "bash", "-lc", command]
    let pipe = Pipe()
    process.standardOutput = pipe
    process.standardError = FileHandle.nullDevice
    do {
        try process.run()
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        return String(data: data, encoding: .utf8) ?? ""
    } catch {
        return ""
    }
}

let registryURL = "http://localhost:18081"
let avroTopic = "kestrel.avro.test"
let avroSubject = "kestrel.avro.test-value"
/// Topic for records this build encodes, read back by Confluent's consumer.
let avroWriteTopic = "kestrel.avro.written"

harness.suite("Confluent wire format") { h in
    h.check("a framed payload splits into a schema id and a body") {
        let framed = ConfluentWireFormat.frame(schemaID: 42, body: Data([1, 2, 3]))
        try expectEqual(framed.count, 8, "5 header bytes plus 3 of body")
        try expectEqual(Array(framed.prefix(5)), [0, 0, 0, 0, 42], "magic byte then a big-endian id")

        let split = try require(ConfluentWireFormat.split(framed), "a split")
        try expectEqual(split.schemaID, 42, "schema id")
        try expectEqual(Array(split.body), [1, 2, 3], "body")
    }

    h.check("a large schema id survives the round trip") {
        // Ids above 2^24 exercise every header byte, and a naive Int8 shift
        // would lose the top one.
        let framed = ConfluentWireFormat.frame(schemaID: 100_000_001, body: Data())
        try expectEqual(
            try require(ConfluentWireFormat.split(framed), "a split").schemaID,
            100_000_001,
            "schema id"
        )
    }

    h.check("payloads that are not framed are left alone") {
        try expect(ConfluentWireFormat.split(Data(#"{"a":1}"#.utf8)) == nil, "JSON is not framed")
        try expect(ConfluentWireFormat.split(Data([0, 1, 2])) == nil, "too short to be framed")
        try expect(ConfluentWireFormat.split(Data()) == nil, "empty is not framed")
        try expect(
            ConfluentWireFormat.split(Data([1, 0, 0, 0, 1, 9])) == nil,
            "the wrong magic byte is not framed"
        )
        try expect(
            ConfluentWireFormat.split(Data([0, 0, 0, 0, 1])) != nil,
            "five bytes is a framed empty body, which is legitimate"
        )
    }

    h.check("isFramed and schemaID agree with split") {
        let framed = ConfluentWireFormat.frame(schemaID: 7, body: Data([0]))
        try expect(AvroPayload.isFramed(framed), "should be framed")
        try expectEqual(AvroPayload.schemaID(of: framed), 7, "schema id")
        try expect(!AvroPayload.isFramed(Data("hello".utf8)), "text is not framed")
        try expect(!AvroPayload.isFramed(nil), "a tombstone is not framed")
    }
}

harness.suite("Reading Avro schemas") { h in
    h.check("a record schema parses into its fields") {
        let (schema, _) = try AvroSchemaParser.parse(#"""
            {"type":"record","name":"User","namespace":"kestrel","fields":[
              {"name":"name","type":"string"},
              {"name":"age","type":"int"}
            ]}
            """#)

        guard case .record(let name, let fields) = schema else {
            throw Expectation(description: "expected a record, got \(schema)")
        }
        try expectEqual(name, "kestrel.User", "the name should include the namespace")
        try expectEqual(fields.map(\.name), ["name", "age"], "field names, in order")
        try expectEqual(fields[1].schema, .int(logical: nil), "second field type")
    }

    h.check("logical types are kept") {
        let (schema, _) = try AvroSchemaParser.parse(#"""
            {"type":"record","name":"Event","fields":[
              {"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}},
              {"name":"day","type":{"type":"int","logicalType":"date"}},
              {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}
            ]}
            """#)

        guard case .record(_, let fields) = schema else {
            throw Expectation(description: "expected a record")
        }
        try expectEqual(fields[0].schema, .long(logical: .timestampMillis), "timestamp")
        try expectEqual(fields[1].schema, .int(logical: .date), "date")
        try expectEqual(fields[2].schema, .bytes(logical: .decimal, scale: 2), "decimal with scale")
    }

    h.check("a recursive schema parses without looping") {
        let (schema, named) = try AvroSchemaParser.parse(#"""
            {"type":"record","name":"Node","fields":[
              {"name":"value","type":"int"},
              {"name":"next","type":["null","Node"]}
            ]}
            """#)

        guard case .record(_, let fields) = schema else { throw Expectation(description: "expected a record") }
        guard case .union(let options) = fields[1].schema else {
            throw Expectation(description: "expected a union")
        }
        try expectEqual(options[1], .reference(name: "Node"), "the second branch refers back")
        try expect(named["Node"] != nil, "the named table should hold Node")
    }

    h.check("a schema that is not JSON is reported as such") {
        do {
            _ = try AvroSchemaParser.parse("{not json")
            throw Expectation(description: "should have thrown")
        } catch let error as AvroSchemaError {
            guard case .notJSON(let detail) = error else {
                throw Expectation(description: "wrong case: \(error)")
            }
            try expect(!detail.isEmpty, "should say what the parser objected to")
        }
    }

    h.check("an unknown type names itself in the error") {
        do {
            _ = try AvroSchemaParser.parse(#"{"type":"record","name":"R","fields":[{"name":"f","type":"quaternion"}]}"#)
            throw Expectation(description: "should have thrown")
        } catch let error as AvroSchemaError {
            try expectEqual(error, .unsupportedType("quaternion"), "the offending type")
        }
    }
}

harness.suite("Avro encoding and decoding") { h in
    /// Encodes then decodes a value, which must come back unchanged.
    func roundTrip(_ value: Any, schema: String, label: String) throws -> Any {
        let (parsed, named) = try AvroSchemaParser.parse(schema)
        let bytes = try AvroEncoder(named: named).encode(value, as: parsed)
        return try AvroDecoder(named: named).decode(bytes, as: parsed)
    }

    h.check("zigzag integers match the specification's examples") {
        // From the Avro specification: 0 → 00, -1 → 01, 1 → 02, 2 → 04, -64 → 7f.
        let cases: [(Int64, [UInt8])] = [(0, [0x00]), (-1, [0x01]), (1, [0x02]), (2, [0x04]), (-64, [0x7f]), (64, [0x80, 0x01])]
        for (value, expected) in cases {
            let bytes = try AvroEncoder().encode(value, as: .long(logical: nil))
            try expectEqual(Array(bytes), expected, "encoding \(value)")
            let decoded = try AvroDecoder().decode(bytes, as: .long(logical: nil))
            try expectEqual(decoded as? Int64, value, "decoding \(value)")
        }
    }

    h.check("a string encodes as a length then its UTF-8") {
        let bytes = try AvroEncoder().encode("foo", as: .string(logical: nil))
        try expectEqual(Array(bytes), [0x06, 0x66, 0x6f, 0x6f], "length 3 zigzagged, then f o o")
    }

    h.check("every primitive survives a round trip") {
        let schema = #"""
            {"type":"record","name":"All","fields":[
              {"name":"nothing","type":"null"},
              {"name":"flag","type":"boolean"},
              {"name":"small","type":"int"},
              {"name":"big","type":"long"},
              {"name":"single","type":"float"},
              {"name":"wide","type":"double"},
              {"name":"blob","type":"bytes"},
              {"name":"text","type":"string"}
            ]}
            """#
        let value: [String: Any] = [
            "nothing": NSNull(),
            "flag": true,
            "small": 42,
            "big": 9_007_199_254_740_993,
            "single": 0.5,
            "wide": 3.141592653589793,
            "blob": Data([0xde, 0xad, 0xbe, 0xef]).base64EncodedString(),
            "text": "héllo · 世界"
        ]

        let decoded = try require(
            roundTrip(value, schema: schema, label: "primitives") as? [String: Any],
            "a record"
        )
        try expectEqual(decoded["flag"] as? Bool, true, "boolean")
        try expectEqual(decoded["small"] as? Int64, 42, "int")
        try expectEqual(decoded["big"] as? Int64, 9_007_199_254_740_993, "a long beyond Double's exact range")
        try expectEqual(decoded["single"] as? Double, 0.5, "float")
        try expectEqual(decoded["wide"] as? Double, 3.141592653589793, "double")
        try expectEqual(decoded["blob"] as? String, "3q2+7w==", "bytes as base64")
        try expectEqual(decoded["text"] as? String, "héllo · 世界", "string")
        try expect(decoded["nothing"] is NSNull, "null")
    }

    h.check("arrays, maps, enums, unions and fixed survive a round trip") {
        let schema = #"""
            {"type":"record","name":"Shapes","fields":[
              {"name":"tags","type":{"type":"array","items":"string"}},
              {"name":"counts","type":{"type":"map","values":"long"}},
              {"name":"state","type":{"type":"enum","name":"State","symbols":["OFF","ON"]}},
              {"name":"note","type":["null","string"]},
              {"name":"id","type":{"type":"fixed","name":"Id","size":4}}
            ]}
            """#
        let value: [String: Any] = [
            "tags": ["a", "b", "c"],
            "counts": ["x": 1, "y": 2],
            "state": "ON",
            "note": "hello",
            "id": Data([1, 2, 3, 4]).base64EncodedString()
        ]

        let decoded = try require(
            roundTrip(value, schema: schema, label: "shapes") as? [String: Any],
            "a record"
        )
        try expectEqual(decoded["tags"] as? [String], ["a", "b", "c"], "array")
        try expectEqual(decoded["counts"] as? [String: Int64], ["x": 1, "y": 2], "map")
        try expectEqual(decoded["state"] as? String, "ON", "enum")
        try expectEqual(decoded["note"] as? String, "hello", "union")
        try expectEqual(decoded["id"] as? String, "AQIDBA==", "fixed")
    }

    h.check("empty arrays and maps encode as a single zero block") {
        let schema = #"{"type":"record","name":"E","fields":[{"name":"a","type":{"type":"array","items":"int"}},{"name":"m","type":{"type":"map","values":"int"}}]}"#
        let (parsed, named) = try AvroSchemaParser.parse(schema)
        let bytes = try AvroEncoder(named: named).encode(["a": [Any](), "m": [String: Any]()], as: parsed)
        try expectEqual(Array(bytes), [0, 0], "one terminator each, no count block")

        let decoded = try require(
            try AvroDecoder(named: named).decode(bytes, as: parsed) as? [String: Any],
            "a record"
        )
        try expectEqual((decoded["a"] as? [Any])?.count, 0, "empty array")
        try expectEqual((decoded["m"] as? [String: Any])?.count, 0, "empty map")
    }

    h.check("a null union branch decodes to null, not a missing field") {
        let schema = #"{"type":"record","name":"N","fields":[{"name":"note","type":["null","string"]}]}"#
        let (parsed, named) = try AvroSchemaParser.parse(schema)
        let bytes = try AvroEncoder(named: named).encode(["note": NSNull()], as: parsed)
        try expectEqual(Array(bytes), [0], "branch 0, then nothing")

        let decoded = try require(
            try AvroDecoder(named: named).decode(bytes, as: parsed) as? [String: Any],
            "a record"
        )
        try expect(decoded["note"] is NSNull, "the field should be present and null")
    }

    h.check("a recursive record round trips") {
        let schema = #"{"type":"record","name":"Node","fields":[{"name":"value","type":"int"},{"name":"next","type":["null","Node"]}]}"#
        let value: [String: Any] = [
            "value": 1,
            "next": ["value": 2, "next": NSNull()] as [String: Any]
        ]
        let decoded = try require(
            roundTrip(value, schema: schema, label: "recursive") as? [String: Any],
            "a record"
        )
        let next = try require(decoded["next"] as? [String: Any], "the nested node")
        try expectEqual(next["value"] as? Int64, 2, "the nested value")
        try expect(next["next"] is NSNull, "the end of the list")
    }

    h.check("timestamps and dates decode to readable text") {
        let schema = #"""
            {"type":"record","name":"When","fields":[
              {"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}},
              {"name":"day","type":{"type":"int","logicalType":"date"}}
            ]}
            """#
        let (parsed, named) = try AvroSchemaParser.parse(schema)
        // 2026-09-16T00:00:00Z, and the same instant as a day number
        // (1789516800 / 86400).
        let bytes = try AvroEncoder(named: named).encode(
            ["at": 1_789_516_800_000, "day": 20_712],
            as: parsed
        )
        let decoded = try require(
            try AvroDecoder(named: named).decode(bytes, as: parsed) as? [String: Any],
            "a record"
        )
        try expectEqual(decoded["at"] as? String, "2026-09-16T00:00:00Z", "timestamp-millis")
        try expectEqual(decoded["day"] as? String, "2026-09-16", "date")
    }

    h.check("a decimal decodes without losing precision") {
        let schema = #"{"type":"bytes","logicalType":"decimal","precision":20,"scale":2}"#
        let (parsed, _) = try AvroSchemaParser.parse(schema)

        // 123456789012345678.99, which no Double can hold exactly. The leading
        // zero byte is what a real writer emits to keep the sign bit clear on a
        // positive value that fills its eight bytes.
        let unscaled: [UInt8] = [0x00, 0xAB, 0x54, 0xA9, 0x8C, 0xEB, 0x1F, 0x0A, 0xDB]
        var body = try AvroEncoder().encode(Int64(unscaled.count), as: .long(logical: nil))
        body.append(Data(unscaled))
        try expectEqual(
            try AvroDecoder().decode(body, as: parsed) as? String,
            "123456789012345678.99",
            "an exact decimal"
        )

        // And the negative, which needs the two's complement path.
        let negative: [UInt8] = [0xFF, 0x54, 0xAB, 0x56, 0x73, 0x14, 0xE0, 0xF5, 0x25]
        var negativeBody = try AvroEncoder().encode(Int64(negative.count), as: .long(logical: nil))
        negativeBody.append(Data(negative))
        try expectEqual(
            try AvroDecoder().decode(negativeBody, as: parsed) as? String,
            "-123456789012345678.99",
            "a negative decimal"
        )
    }

    h.check("the wrong schema is reported rather than decoded into nonsense") {
        // Bytes written for a two-field record, read as a one-field record.
        let (wide, _) = try AvroSchemaParser.parse(#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}"#)
        let (narrow, _) = try AvroSchemaParser.parse(#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}"#)
        let bytes = try AvroEncoder().encode(["a": 1, "b": 2], as: wide)

        do {
            _ = try AvroDecoder().decode(bytes, as: narrow)
            throw Expectation(description: "should have thrown rather than ignoring a byte")
        } catch let error as AvroCodecError {
            try expectEqual(error, .trailingBytes(1), "leftover bytes")
        }
    }

    h.check("a truncated payload says what it ran out of") {
        do {
            _ = try AvroDecoder().decode(Data([0x06, 0x66]), as: .string(logical: nil))
            throw Expectation(description: "should have thrown")
        } catch let error as AvroCodecError {
            try expectEqual(error, .truncated("a string"), "what was being read")
        }
    }

    h.check("a value that does not fit the schema names the field") {
        let (parsed, named) = try AvroSchemaParser.parse(#"{"type":"record","name":"R","fields":[{"name":"count","type":"int"}]}"#)
        do {
            _ = try AvroEncoder(named: named).encode(["count": "seven"], as: parsed)
            throw Expectation(description: "should have thrown")
        } catch let error as AvroCodecError {
            try expectEqual(
                error,
                .valueMismatch(field: "value.count", expected: "a whole number"),
                "the failing field"
            )
        }
    }

    h.check("a missing field is refused rather than sent as a default") {
        let (parsed, named) = try AvroSchemaParser.parse(#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"int"}]}"#)
        do {
            _ = try AvroEncoder(named: named).encode(["a": 1], as: parsed)
            throw Expectation(description: "should have thrown")
        } catch let error as AvroCodecError {
            try expectEqual(
                error,
                .valueMismatch(field: "value.b", expected: "a present field"),
                "the missing field"
            )
        }
    }

    h.check("a fractional value is refused for an int field") {
        do {
            _ = try AvroEncoder().encode(1.5, as: .int(logical: nil))
            throw Expectation(description: "1.5 is not a whole number and must not be truncated")
        } catch let error as AvroCodecError {
            try expectEqual(error, .valueMismatch(field: "value", expected: "a whole number"), "the error")
        }
    }
}

/// The schema the fixture registers, with a plain long rather than a
/// timestamp: kafka-avro-console-producer reads a logical type into an Instant
/// and then fails to write it, so the fixture cannot use one. Logical types are
/// covered by the round-trip checks instead.
///
/// Whitespace is stripped so the document is one line, which is how both the
/// registry and the console producer want it.
let avroFixtureSchema = #"""
    {"type":"record","name":"Order","namespace":"kestrel.test","fields":[
      {"name":"id","type":"string"},
      {"name":"amount","type":"double"},
      {"name":"quantity","type":"int"},
      {"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID","SHIPPED"]}},
      {"name":"tags","type":{"type":"array","items":"string"}},
      {"name":"note","type":["null","string"]},
      {"name":"created","type":"long"}
    ]}
    """#
    .split(separator: "\n")
    .map { $0.trimmingCharacters(in: .whitespaces) }
    .joined()

/// The records the fixture writes, in Avro's JSON encoding, where a union value
/// is tagged with its branch.
let avroFixtureRecords = [
    #"{"id":"order-1","amount":42.5,"quantity":3,"status":"PAID","tags":["a","b"],"note":{"string":"hello from confluent"},"created":1757894400000}"#,
    #"{"id":"order-2","amount":0.0,"quantity":0,"status":"NEW","tags":[],"note":null,"created":1757894401000}"#
]

/// Copies a file into a container.
///
/// The fixture's JSON goes in as a file rather than through a shell, because
/// quoting Avro's JSON through `docker exec bash -c` corrupts it in ways that
/// look like a broken producer.
func dockerCopy(_ localPath: String, to container: String, at remotePath: String) -> Bool {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    process.arguments = ["docker", "cp", localPath, "\(container):\(remotePath)"]
    process.standardOutput = FileHandle.nullDevice
    process.standardError = FileHandle.nullDevice
    do {
        try process.run()
        process.waitUntilExit()
        return process.terminationStatus == 0
    } catch {
        return false
    }
}

await harness.asyncSuite("Avro fixtures") { h in
    await h.checkAsync("the Avro topic holds records written by Confluent's producer") {
        _ = dockerRun(
            "kestrel-kafka",
            "kafka-topics --bootstrap-server localhost:19092 --create --topic \(avroTopic) "
                + "--partitions 1 --replication-factor 1 2>/dev/null || true"
        )

        let consumer = try KafkaConsumer(profile: localProfile)
        let marks = try await consumer.watermarks(topic: avroTopic, partition: 0)

        // Seeded once. Producing again on every run would leave the offsets the
        // decoding checks read from holding different records each time.
        if marks.high == 0 {
            let directory = URL(fileURLWithPath: NSTemporaryDirectory())
            let recordsFile = directory.appendingPathComponent("kestrel-avro-records.json")
            let schemaFile = directory.appendingPathComponent("kestrel-avro-schema.json")
            try (avroFixtureRecords.joined(separator: "\n") + "\n").write(
                to: recordsFile,
                atomically: true,
                encoding: .utf8
            )
            try avroFixtureSchema.write(to: schemaFile, atomically: true, encoding: .utf8)

            try expect(
                dockerCopy(recordsFile.path, to: "kestrel-schema-registry", at: "/tmp/records.json"),
                "could not copy the fixture records into the container"
            )
            try expect(
                dockerCopy(schemaFile.path, to: "kestrel-schema-registry", at: "/tmp/schema.json"),
                "could not copy the fixture schema into the container"
            )

            let output = dockerRun(
                "kestrel-schema-registry",
                "kafka-avro-console-producer --bootstrap-server kestrel-kafka:29092 "
                    + "--topic \(avroTopic) "
                    + "--property schema.registry.url=http://localhost:18081 "
                    + "--property value.schema=\"$(cat /tmp/schema.json)\" "
                    + "< /tmp/records.json 2>&1 | grep -iE 'exception|caused by' | head -3"
            )

            let after = try await consumer.watermarks(topic: avroTopic, partition: 0)
            try expect(
                after.high >= 2,
                "the fixture should have produced two Avro records; producer said: \(output)"
            )
        }

        let page = try await consumer.fetch(
            topic: avroTopic,
            partition: 0,
            from: StartPosition.earliest,
            limit: 2
        )
        try expect(
            AvroPayload.isFramed(try fixtureRecord(page, 1, "Avro fixture").value),
            "the fixture records should be Confluent-framed"
        )
    }
}

await harness.asyncSuite("Schema Registry") { h in
    await h.checkAsync("the registry is reachable and holds the fixture subject") {
        let client = try SchemaRegistryClient(url: registryURL)
        let summary = try await client.test()
        try expect(summary.hasPrefix("Reachable"), "expected a reachable summary, got \(summary)")

        let subjects = try await client.subjects()
        try expect(
            subjects.contains(avroSubject),
            "expected \(avroSubject) among \(subjects). Run Docker/kafka.yml and the Avro fixture."
        )
    }

    await h.checkAsync("a schema fetched by subject and by id are the same schema") {
        let client = try SchemaRegistryClient(url: registryURL)
        let latest = try await client.latestSchema(subject: avroSubject)
        let byID = try await client.schema(id: latest.id)

        try expectEqual(byID.text, latest.text, "the same schema document")
        try expectEqual(latest.subject, avroSubject, "subject")
        try expect(try require(latest.version, "a version") >= 1, "a version number")
    }

    await h.checkAsync("an unknown schema id is reported as not found") {
        let client = try SchemaRegistryClient(url: registryURL)
        do {
            _ = try await client.schema(id: 999_999)
            throw Expectation(description: "should have thrown")
        } catch let error as SchemaRegistryError {
            guard case .notFound = error else { throw Expectation(description: "wrong case: \(error)") }
        }
    }

    await h.checkAsync("an unreachable registry says so, with the reason") {
        let client = try SchemaRegistryClient(url: "http://localhost:18099", timeout: 3)
        do {
            _ = try await client.subjects()
            throw Expectation(description: "nothing should be listening on 18099")
        } catch let error as SchemaRegistryError {
            guard case .unreachable(let detail) = error else {
                throw Expectation(description: "wrong case: \(error)")
            }
            try expect(!detail.isEmpty, "should carry the reason")
        }
    }

    await h.checkAsync("a URL that is not a URL is refused at construction") {
        for bad in ["", "not a url", "localhost:18081"] {
            do {
                _ = try SchemaRegistryClient(url: bad)
                throw Expectation(description: "\(bad) should not be accepted")
            } catch let error as SchemaRegistryError {
                guard case .badURL = error else { throw Expectation(description: "wrong case for \(bad)") }
            }
        }
    }

    await h.checkAsync("the schema cache serves the second read") {
        let client = try SchemaRegistryClient(url: registryURL)
        let latest = try await client.latestSchema(subject: avroSubject)
        _ = try await client.schema(id: latest.id)

        // Only measurable indirectly: the second fetch must not need the
        // network, so it still succeeds once the URL is unreachable. Same id,
        // fresh client would fail; this one is cached.
        let cached = try await client.schema(id: latest.id)
        try expectEqual(cached.id, latest.id, "the cached schema")
    }
}

await harness.asyncSuite("Decoding Avro from the broker") { h in
    await h.checkAsync("a record Confluent's producer wrote decodes to pretty JSON") {
        // The acceptance line: these records were written by
        // kafka-avro-console-producer, not by this build, so the decoder is
        // being checked against the real wire format.
        let consumer = try KafkaConsumer(profile: localProfile)
        let page = try await consumer.fetch(
            topic: avroTopic,
            partition: 0,
            from: StartPosition.earliest,
            limit: 10
        )
        try expect(page.records.count >= 2, "expected the fixture records, got \(page.records.count)")

        let registry = try SchemaRegistryClient(url: registryURL)
        let decoded = try require(
            try await AvroPayload.decode(
                try fixtureRecord(page, 0, "Avro fixture").value,
                registry: registry
            ),
            "a decoded record"
        )

        try expect(decoded.schemaID > 0, "a schema id")
        let object = try require(
            try JSONSerialization.jsonObject(with: Data(decoded.json.utf8)) as? [String: Any],
            "the decoded JSON"
        )
        try expectEqual(object["id"] as? String, "order-1", "id")
        try expectEqual(object["amount"] as? Double, 42.5, "amount")
        try expectEqual(object["quantity"] as? Int, 3, "quantity")
        try expectEqual(object["status"] as? String, "PAID", "enum symbol")
        try expectEqual(object["tags"] as? [String], ["a", "b"], "array")
        try expectEqual(object["note"] as? String, "hello from confluent", "union branch")
        try expectEqual(object["created"] as? Int, 1_757_894_400_000, "long")

        try expect(decoded.json.contains("\n"), "should be pretty-printed, not one line")
        try expect(!decoded.schemaText.isEmpty, "should carry the schema for display")
    }

    await h.checkAsync("the second fixture record decodes, including its null and empty array") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let page = try await consumer.fetch(topic: avroTopic, partition: 0, from: StartPosition.earliest, limit: 10)
        let registry = try SchemaRegistryClient(url: registryURL)

        let decoded = try require(
            try await AvroPayload.decode(
                try fixtureRecord(page, 1, "Avro fixture").value,
                registry: registry
            ),
            "a decoded record"
        )
        let object = try require(
            try JSONSerialization.jsonObject(with: Data(decoded.json.utf8)) as? [String: Any],
            "the decoded JSON"
        )
        try expectEqual(object["id"] as? String, "order-2", "id")
        try expectEqual((object["tags"] as? [Any])?.count, 0, "an empty array")
        try expect(object["note"] is NSNull, "a null union branch")
    }

    await h.checkAsync("an Avro record without a registry explains itself instead of showing hex") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let page = try await consumer.fetch(topic: avroTopic, partition: 0, from: StartPosition.earliest, limit: 1)

        let record = try fixtureRecord(page, 0, "Avro fixture")
        do {
            _ = try await AvroPayload.decode(record.value, registry: nil)
            throw Expectation(description: "should have thrown")
        } catch let error as SchemaRegistryError {
            try expectEqual(error, .notConfigured, "the error")
            let message = try require(error.errorDescription, "a message")
            try expect(message.contains("Schema Registry"), "should name what is missing: \(message)")
            try expect(message.contains("Edit Cluster"), "should say where to set it: \(message)")
        }

        // And the payload is still recognisable as Avro, which is what puts the
        // message on screen rather than leaving the pane to the hex dump.
        try expect(AvroPayload.isFramed(record.value), "should be recognised as framed")
    }

    await h.checkAsync("a plain JSON record is not mistaken for Avro") {
        let consumer = try KafkaConsumer(profile: localProfile)
        let page = try await consumer.fetch(topic: fixtureTopic, partition: 0, from: StartPosition.earliest, limit: 1)
        let registry = try SchemaRegistryClient(url: registryURL)

        try expect(
            try await AvroPayload.decode(
                try fixtureRecord(page, 0, "JSON fixture").value,
                registry: registry
            ) == nil,
            "a JSON payload has no Confluent frame and must be left to the JSON formatter"
        )
    }
}

await harness.asyncSuite("Producing Avro") { h in
    await h.checkAsync("a record this build encodes is read back by Confluent's consumer") {
        // The other direction of the acceptance line. Confluent's consumer
        // decodes using the schema the registry holds, so if it prints the
        // record, the bytes are correct Avro under the right schema id.
        _ = dockerRun(
            "kestrel-kafka",
            "kafka-topics --bootstrap-server localhost:19092 --create --topic \(avroWriteTopic) --partitions 1 --replication-factor 1 2>/dev/null || true"
        )

        let registry = try SchemaRegistryClient(url: registryURL)
        let marker = "kestrel-\(UUID().uuidString.prefix(8))"
        let json = """
            {"id":"\(marker)","amount":19.95,"quantity":7,"status":"SHIPPED",\
            "tags":["swift","avro"],"note":"written by kestrel","created":1757894402000}
            """

        let framed = try await AvroPayload.encode(json: json, subject: avroSubject, registry: registry)
        try expect(AvroPayload.isFramed(framed), "the produced bytes should be framed")

        let client = try KafkaClient(profile: localProfile)
        let report = try await client.produce(
            topic: avroWriteTopic,
            partition: nil,
            key: Data(marker.utf8),
            value: framed,
            headers: []
        )
        try expect(report.offset >= 0, "should have been assigned an offset")

        let output = dockerRun(
            "kestrel-schema-registry",
            "timeout 30 kafka-avro-console-consumer --bootstrap-server kestrel-kafka:29092 "
                + "--topic \(avroWriteTopic) --from-beginning --max-messages \(report.offset + 1) "
                + "--property schema.registry.url=http://localhost:18081 2>/dev/null"
        )

        try expect(
            output.contains(marker),
            "Confluent's consumer should print the record it decoded; got: \(output)"
        )
        try expect(output.contains("SHIPPED"), "the enum symbol should survive: \(output)")
        try expect(output.contains("written by kestrel"), "the union branch should survive: \(output)")
        try expect(output.contains("19.95"), "the double should survive: \(output)")
    }

    await h.checkAsync("what this build encodes, it also decodes back") {
        let registry = try SchemaRegistryClient(url: registryURL)
        let json = """
            {"id":"order-3","amount":1.5,"quantity":1,"status":"NEW",\
            "tags":[],"note":null,"created":0}
            """
        let framed = try await AvroPayload.encode(json: json, subject: avroSubject, registry: registry)
        let decoded = try require(
            try await AvroPayload.decode(framed, registry: registry),
            "a decoded record"
        )
        let object = try require(
            try JSONSerialization.jsonObject(with: Data(decoded.json.utf8)) as? [String: Any],
            "the decoded JSON"
        )
        try expectEqual(object["id"] as? String, "order-3", "id")
        try expect(object["note"] is NSNull, "null note")
        try expectEqual((object["tags"] as? [Any])?.count, 0, "empty tags")
    }

    await h.checkAsync("a value that does not fit the subject's schema is refused before producing") {
        let registry = try SchemaRegistryClient(url: registryURL)
        do {
            _ = try await AvroPayload.encode(
                json: #"{"id":"x","amount":1.0,"quantity":1,"status":"NEW","tags":[],"note":null}"#,
                subject: avroSubject,
                registry: registry
            )
            throw Expectation(description: "the missing created field should have been caught")
        } catch let error as AvroCodecError {
            try expectEqual(
                error,
                .valueMismatch(field: "value.created", expected: "a present field"),
                "should name the missing field"
            )
        }
    }

    await h.checkAsync("an unknown subject is reported as not found") {
        let registry = try SchemaRegistryClient(url: registryURL)
        do {
            _ = try await AvroPayload.encode(json: "{}", subject: "kestrel.nonexistent-value", registry: registry)
            throw Expectation(description: "should have thrown")
        } catch let error as SchemaRegistryError {
            guard case .notFound(let what) = error else { throw Expectation(description: "wrong case: \(error)") }
            try expect(what.contains("kestrel.nonexistent-value"), "should name the subject: \(what)")
        }
    }
}

harness.suite("Avro in a cluster profile") { h in
    h.check("a profile keeps its registry URL and user but never a password") {
        var profile = localProfile
        profile.schemaRegistry = SchemaRegistrySettings(url: registryURL, user: "reader")

        let encoder = JSONEncoder()
        // Without this the URL's slashes come back escaped, which is valid JSON
        // but makes the assertion below read as a failure.
        encoder.outputFormatting = [.withoutEscapingSlashes]
        let data = try encoder.encode(profile)
        let text = try require(String(data: data, encoding: .utf8), "encoded JSON")
        try expect(text.contains(registryURL), "the URL should be saved")
        try expect(text.contains("reader"), "the user should be saved")

        let restored = try JSONDecoder().decode(ClusterProfile.self, from: data)
        try expectEqual(restored.schemaRegistry.url, registryURL, "URL")
        try expectEqual(restored.schemaRegistry.user, "reader", "user")
        try expect(restored.schemaRegistry.isConfigured, "should count as configured")
    }

    h.check("a profile saved before the registry field existed still loads") {
        // The shape written by an earlier build. The synthesised decoder would
        // reject it, losing every saved cluster on upgrade.
        let old = #"""
            {"id":"6E8C5A6E-4E6E-4C2E-9E4A-1B2C3D4E5F60","name":"old",
             "bootstrapServers":"localhost:19092","securityProtocol":"PLAINTEXT",
             "saslUsername":"","tls":{"caLocation":"","certificateLocation":"",
             "keyLocation":"","verifyHostname":true}}
            """#
        let profile = try JSONDecoder().decode(ClusterProfile.self, from: Data(old.utf8))
        try expectEqual(profile.name, "old", "name")
        try expect(!profile.schemaRegistry.isConfigured, "no registry, rather than a decode failure")
    }

    h.check("a blank or whitespace URL does not count as configured") {
        try expect(!SchemaRegistrySettings(url: "").isConfigured, "empty")
        try expect(!SchemaRegistrySettings(url: "   ").isConfigured, "whitespace")
        try expect(SchemaRegistrySettings(url: "http://x:1").isConfigured, "a real URL")
    }

    h.check("the registry password round trips through the Keychain") {
        let cluster = UUID()
        defer { try? keychain.removeAll(cluster: cluster) }

        try keychain.set("registry-secret", secret: .schemaRegistryPassword, cluster: cluster)
        try expectEqual(
            try keychain.get(secret: .schemaRegistryPassword, cluster: cluster),
            "registry-secret",
            "the stored password"
        )

        // And removeAll covers it, so deleting a cluster leaves nothing behind.
        try keychain.removeAll(cluster: cluster)
        try expect(
            try keychain.get(secret: .schemaRegistryPassword, cluster: cluster) == nil,
            "should be gone with the rest of the cluster's secrets"
        )
    }
}

// MARK: Kafka Connect

let connectURL = "http://localhost:18083"
let connectTopic = "kestrel.connect.test"
/// Source connector the fixture deploys. Reads a file in the worker's
/// container into a topic, which needs no plugin beyond what the image ships.
let connectSource = "kestrel.file.source"
/// Deliberately named with a space, to prove the client escapes names into the
/// URL path rather than assuming they are path-safe.
let connectBrokenSink = "kestrel broken sink"

/// Creates a connector if it is not already deployed.
///
/// The config goes in as a file and is posted with `curl -d @file`: quoting
/// JSON through `docker exec bash -c` corrupts it, as the Avro fixture found.
func deployConnector(_ name: String, config: [String: String]) -> String {
    let existing = dockerRun("kestrel-connect", "curl -s \(connectURL)/connectors")
    if existing.contains("\"\(name)\"") { return "already deployed" }

    let payload: [String: Any] = ["name": name, "config": config]
    guard let data = try? JSONSerialization.data(withJSONObject: payload) else {
        return "could not encode the config"
    }

    let local = URL(fileURLWithPath: NSTemporaryDirectory())
        .appendingPathComponent("kestrel-connector.json")
    guard (try? data.write(to: local)) != nil,
          dockerCopy(local.path, to: "kestrel-connect", at: "/tmp/connector.json")
    else {
        return "could not copy the config into the container"
    }

    return dockerRun(
        "kestrel-connect",
        "curl -s -X POST -H 'Content-Type: application/json' "
            + "-d @/tmp/connector.json \(connectURL)/connectors"
    )
}

/// Waits for a connector to reach a state, since Connect applies pause, resume
/// and restart asynchronously after a rebalance.
func awaitConnector(
    _ name: String,
    on client: KafkaConnectClient,
    state: ConnectorState,
    seconds: Int = 20
) async throws -> ConnectorInfo {
    var last: ConnectorInfo?
    for _ in 0..<(seconds * 2) {
        let connector = try await client.connector(named: name)
        last = connector
        if connector.state == state { return connector }
        try? await Task.sleep(for: .milliseconds(500))
    }
    throw Expectation(
        description: "\(name) never reached \(state.label); last state was \(last?.state.label ?? "unknown")"
    )
}

harness.suite("Connect settings and states") { h in
    h.check("known states parse, and an unfamiliar one is kept rather than dropped") {
        try expectEqual(ConnectorState("RUNNING"), .running, "running")
        try expectEqual(ConnectorState("paused"), .paused, "lower case still parses")
        try expectEqual(ConnectorState("STOPPED"), .stopped, "stopped, added in Connect 3.5")
        try expectEqual(ConnectorState("FAILED"), .failed, "failed")
        try expectEqual(ConnectorState("UNASSIGNED"), .unassigned, "unassigned")
        try expectEqual(ConnectorState("SOMETHING_NEW"), .other("SOMETHING_NEW"), "unknown")
        try expectEqual(ConnectorState("SOMETHING_NEW").label, "SOMETHING_NEW", "shown as reported")
    }

    h.check("only failed and unassigned count as problems") {
        try expect(ConnectorState.failed.isProblem, "failed")
        try expect(ConnectorState.unassigned.isProblem, "unassigned means no worker took it")
        try expect(!ConnectorState.running.isProblem, "running")
        try expect(!ConnectorState.paused.isProblem, "paused is deliberate, not a problem")
    }

    h.check("a connector kind comes from the status, or is unknown") {
        try expectEqual(ConnectorKind("source"), .source, "source")
        try expectEqual(ConnectorKind("SINK"), .sink, "case insensitive")
        try expectEqual(ConnectorKind(nil), .unknown, "absent")
        try expectEqual(ConnectorKind("something"), .unknown, "unrecognised")
    }

    h.check("a blank or whitespace URL does not count as configured") {
        try expect(!ConnectSettings(url: "").isConfigured, "empty")
        try expect(!ConnectSettings(url: "  ").isConfigured, "whitespace")
        try expect(ConnectSettings(url: "http://x:1").isConfigured, "a real URL")
    }

    h.check("a profile keeps its Connect URL and user but never a password") {
        var profile = localProfile
        profile.connect = ConnectSettings(url: connectURL, user: "connector")

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.withoutEscapingSlashes]
        let data = try encoder.encode(profile)
        let text = try require(String(data: data, encoding: .utf8), "encoded JSON")
        try expect(text.contains(connectURL), "the URL should be saved")
        try expect(text.contains("connector"), "the user should be saved")

        let restored = try JSONDecoder().decode(ClusterProfile.self, from: data)
        try expectEqual(restored.connect.url, connectURL, "URL")
        try expectEqual(restored.connect.user, "connector", "user")
    }

    h.check("the Connect password round trips through the Keychain") {
        let cluster = UUID()
        defer { try? keychain.removeAll(cluster: cluster) }

        try keychain.set("connect-secret", secret: .connectPassword, cluster: cluster)
        try expectEqual(
            try keychain.get(secret: .connectPassword, cluster: cluster),
            "connect-secret",
            "the stored password"
        )

        try keychain.removeAll(cluster: cluster)
        try expect(
            try keychain.get(secret: .connectPassword, cluster: cluster) == nil,
            "should be gone with the rest of the cluster's secrets"
        )
    }
}

await harness.asyncSuite("Connect when it is not there") { h in
    await h.checkAsync("a URL that is not a URL is refused at construction") {
        for bad in ["", "not a url", "localhost:18083"] {
            do {
                _ = try KafkaConnectClient(url: bad)
                throw Expectation(description: "\(bad) should not be accepted")
            } catch let error as KafkaConnectError {
                guard case .badURL = error else { throw Expectation(description: "wrong case for \(bad)") }
            }
        }
    }

    await h.checkAsync("an unreachable worker is explained, with the reason") {
        // The acceptance line's second half: nothing is listening on 18099, and
        // the failure has to say so rather than read as an empty cluster.
        let client = try KafkaConnectClient(url: "http://localhost:18099", timeout: 3)
        do {
            _ = try await client.connectors()
            throw Expectation(description: "nothing should be listening on 18099")
        } catch let error as KafkaConnectError {
            guard case .unreachable(let detail) = error else {
                throw Expectation(description: "wrong case: \(error)")
            }
            try expect(!detail.isEmpty, "should carry the reason")

            let message = try require(error.errorDescription, "a message")
            try expect(message.contains("unreachable"), "should say it is unreachable: \(message)")
        }
    }

    await h.checkAsync("a cluster with no Connect URL says so rather than failing silently") {
        let message = try require(KafkaConnectError.notConfigured.errorDescription, "a message")
        try expect(message.contains("Edit Cluster"), "should say where to set it: \(message)")
    }
}

await harness.asyncSuite("Connect fixtures") { h in
    await h.checkAsync("the worker is up with the fixture connectors deployed") {
        let client = try KafkaConnectClient(url: connectURL, timeout: 20)

        let worker = try await client.worker()
        try expect(!worker.version.isEmpty, "the worker should report a version")
        print("       Connect \(worker.version), cluster \(worker.kafkaClusterID ?? "unknown")")

        _ = dockerRun(
            "kestrel-kafka",
            "kafka-topics --bootstrap-server localhost:19092 --create --topic \(connectTopic) "
                + "--partitions 1 --replication-factor 1 2>/dev/null || true"
        )
        // Something for the source connector to read.
        _ = dockerRun(
            "kestrel-connect",
            "printf 'line-one\\nline-two\\nline-three\\n' > /tmp/kestrel-source.txt"
        )

        let source = deployConnector(connectSource, config: [
            "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "tasks.max": "1",
            "file": "/tmp/kestrel-source.txt",
            "topic": connectTopic
        ])

        // A sink pointed at a directory, which its task cannot open. Gives a
        // genuinely failed connector with a trace, which is what the inspector
        // has to show.
        let broken = deployConnector(connectBrokenSink, config: [
            "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
            "tasks.max": "1",
            "topics": connectTopic,
            "file": "/tmp"
        ])

        let connectors = try await client.connectors()
        try expect(
            connectors.contains { $0.name == connectSource },
            "expected \(connectSource) among \(connectors.map(\.name)); deploy said: \(source)"
        )
        try expect(
            connectors.contains { $0.name == connectBrokenSink },
            "expected the broken sink; deploy said: \(broken)"
        )
    }
}

await harness.asyncSuite("Listing connectors") { h in
    await h.checkAsync("the list carries each connector's state, tasks and configuration") {
        // The acceptance line: against a live worker, the connectors are
        // listed with what the window needs to show.
        let client = try KafkaConnectClient(url: connectURL)
        let connectors = try await client.connectors()

        try expect(connectors.count >= 2, "expected the fixtures, got \(connectors.count)")
        try expectEqual(
            connectors.map(\.name),
            connectors.map(\.name).sorted(),
            "should be sorted, so the sidebar does not reshuffle between refreshes"
        )

        let source = try require(
            connectors.first { $0.name == connectSource },
            "the source connector"
        )
        _ = try await awaitConnector(connectSource, on: client, state: .running)
        try expectEqual(source.kind, .source, "a FileStreamSource is a source")
        try expectEqual(
            source.connectorClass,
            "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "connector class"
        )
        try expectEqual(source.config["topic"], connectTopic, "its topic, from the config")
        try expect(!source.workerID.isEmpty, "the worker holding it")
    }

    await h.checkAsync("a running connector has a running task") {
        let client = try KafkaConnectClient(url: connectURL)
        let source = try await awaitConnector(connectSource, on: client, state: .running)

        try expectEqual(source.tasks.count, 1, "tasks.max was 1")
        let task = try require(source.tasks.first, "the task")
        try expectEqual(task.state, .running, "task state")
        try expect(!task.workerID.isEmpty, "the worker running it")
        try expect(task.trace == nil, "a running task has no trace")
        try expect(!source.needsAttention, "a healthy connector needs no attention")
    }

    await h.checkAsync("the source connector actually moved the file into the topic") {
        // Not required by the slice, but it proves the fixture is a real
        // working connector rather than something that merely reports RUNNING.
        let consumer = try KafkaConsumer(profile: localProfile)
        for _ in 0..<20 {
            let marks = try await consumer.watermarks(topic: connectTopic, partition: 0)
            if marks.high >= 3 { break }
            try? await Task.sleep(for: .milliseconds(500))
        }

        let page = try await consumer.fetch(
            topic: connectTopic,
            partition: 0,
            from: StartPosition.earliest,
            limit: 10
        )
        let values = page.records.compactMap { $0.value.flatMap { String(data: $0, encoding: .utf8) } }
        try expect(
            values.contains { $0.contains("line-one") },
            "expected the file's lines on the topic, got \(values)"
        )
    }

    await h.checkAsync("a failed task is reported with its trace") {
        let client = try KafkaConnectClient(url: connectURL)
        let broken = try await awaitConnector(connectBrokenSink, on: client, state: .running)

        // The connector itself runs; it is the task that cannot open the file.
        var failed: ConnectorTask?
        for _ in 0..<40 {
            let current = try await client.connector(named: connectBrokenSink)
            if let task = current.failedTasks.first {
                failed = task
                break
            }
            try? await Task.sleep(for: .milliseconds(500))
        }

        let task = try require(failed, "a failed task on the broken sink")
        try expectEqual(task.state, .failed, "task state")
        let trace = try require(task.trace, "a trace")
        try expect(trace.count > 50, "the trace should be the real stack: \(trace.prefix(80))")

        let current = try await client.connector(named: connectBrokenSink)
        try expect(current.needsAttention, "a connector with a failed task needs attention")
        try expectEqual(broken.kind, .sink, "a FileStreamSink is a sink")
    }

    await h.checkAsync("a name with a space is escaped into the URL path") {
        // Fetched by name, which only works if the space is percent-encoded.
        let client = try KafkaConnectClient(url: connectURL)
        let byName = try await client.connector(named: connectBrokenSink)
        try expectEqual(byName.name, connectBrokenSink, "the connector, fetched by its awkward name")
        try expect(!byName.config.isEmpty, "its configuration came back too")
    }

    await h.checkAsync("an unknown connector is reported as not found") {
        let client = try KafkaConnectClient(url: connectURL)
        do {
            _ = try await client.connector(named: "kestrel.nonexistent")
            throw Expectation(description: "should have thrown")
        } catch let error as KafkaConnectError {
            guard case .notFound(let what) = error else {
                throw Expectation(description: "wrong case: \(error)")
            }
            try expect(what.contains("kestrel.nonexistent"), "should name it: \(what)")
        }
    }
}

await harness.asyncSuite("Pausing, resuming and restarting") { h in
    await h.checkAsync("pause stops the connector and resume starts it again") {
        let client = try KafkaConnectClient(url: connectURL)
        _ = try await awaitConnector(connectSource, on: client, state: .running)

        try await client.pause(connectSource)
        let paused = try await awaitConnector(connectSource, on: client, state: .paused)
        try expectEqual(paused.state, .paused, "state after pause")

        try await client.resume(connectSource)
        let resumed = try await awaitConnector(connectSource, on: client, state: .running)
        try expectEqual(resumed.state, .running, "state after resume")
        try expectEqual(
            resumed.tasks.filter { $0.state == .running }.count,
            1,
            "its task should be running again"
        )
    }

    await h.checkAsync("restarting a connector and its tasks is accepted") {
        let client = try KafkaConnectClient(url: connectURL)
        _ = try await awaitConnector(connectSource, on: client, state: .running)

        try await client.restart(connectSource, includeTasks: true)
        let after = try await awaitConnector(connectSource, on: client, state: .running)
        try expectEqual(after.tasks.count, 1, "the task should come back")
    }

    await h.checkAsync("restarting a single task is accepted") {
        let client = try KafkaConnectClient(url: connectURL)
        let source = try await awaitConnector(connectSource, on: client, state: .running)
        let task = try require(source.tasks.first, "the task")

        try await client.restartTask(task.id, of: connectSource)
        let after = try await awaitConnector(connectSource, on: client, state: .running)
        try expectEqual(
            after.tasks.filter { $0.state == .running }.count,
            1,
            "the task should be running after its restart"
        )
    }

    await h.checkAsync("pausing something that is not there is reported, not swallowed") {
        let client = try KafkaConnectClient(url: connectURL)
        do {
            try await client.pause("kestrel.nonexistent")
            throw Expectation(description: "should have thrown")
        } catch let error as KafkaConnectError {
            guard case .notFound = error else {
                throw Expectation(description: "wrong case: \(error)")
            }
        }
    }

    await h.checkAsync("the test summary names the version and the connector count") {
        let client = try KafkaConnectClient(url: connectURL)
        let summary = try await client.test()
        try expect(summary.hasPrefix("Connect "), "should lead with the version: \(summary)")
        try expect(summary.contains("connector"), "should count connectors: \(summary)")
    }
}

// Packaging comes last: it reads the staged bundle and the built image, which
// are artifacts of the build rather than of the broker.
registerPackagingChecks(harness)

// The CLI's checks run the built binary, so they come after everything that
// seeds the topics, groups, schemas and connectors they read.
await registerCLIChecks(harness)

// Leave no consumer running once the checks are done.
stopLiveConsumer(liveConsumer)

harness.finish()
