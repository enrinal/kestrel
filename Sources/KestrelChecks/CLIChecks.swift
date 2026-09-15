import Foundation
import KestrelKit

/// What running `kestrel` produced.
struct CLIResult {
    let status: Int32
    let out: String
    let err: String

    /// Standard output and standard error together, for checks that only care
    /// that a sentence appeared somewhere.
    var all: String { out + err }

    /// Output lines with the table's heading and rule dropped.
    var rows: [String] {
        out.split(separator: "\n", omittingEmptySubsequences: true)
            .dropFirst(2)
            .map(String.init)
    }
}

/// Runs the built `kestrel` binary.
///
/// The binary is run rather than the command functions being called directly,
/// because half of what the CLI has to get right — exit codes, which stream a
/// message goes to, how the shell splits arguments — is invisible from inside
/// the process.
func kestrel(_ arguments: [String], timeout: TimeInterval = 60) -> CLIResult {
    let binary = URL(fileURLWithPath: ".build/debug/kestrel").standardizedFileURL

    let process = Process()
    process.executableURL = binary
    process.arguments = arguments
    let out = Pipe()
    let err = Pipe()
    process.standardOutput = out
    process.standardError = err

    do {
        try process.run()
    } catch {
        return CLIResult(status: -1, out: "", err: "could not run \(binary.path): \(error)")
    }

    // Read both pipes before waiting: a command that fills one of them while
    // nobody is reading deadlocks against the 64 KiB pipe buffer, and `find`
    // over every topic produces more than that.
    let outData = out.fileHandleForReading.readDataToEndOfFile()
    let errData = err.fileHandleForReading.readDataToEndOfFile()

    let deadline = Date().addingTimeInterval(timeout)
    while process.isRunning, Date() < deadline {
        usleep(50_000)
    }
    if process.isRunning {
        process.terminate()
        return CLIResult(status: -2, out: "", err: "timed out after \(Int(timeout))s")
    }

    return CLIResult(
        status: process.terminationStatus,
        out: String(data: outData, encoding: .utf8) ?? "",
        err: String(data: errData, encoding: .utf8) ?? ""
    )
}

/// Runs `kestrel` against the local cluster profile.
func kestrelLocal(_ arguments: [String], timeout: TimeInterval = 60) -> CLIResult {
    kestrel(arguments + ["--cluster", "local"], timeout: timeout)
}

let cliTopic = "kestrel.cli.test"
let cliRoundTripTopic = "kestrel.cli.roundtrip"

@MainActor
func registerCLIChecks(_ harness: Harness) async {
    harness.suite("The CLI binary") { h in
        // The app target is KestrelApp and not Kestrel for this reason, and a
        // check earns its place here because the failure is silent: `kestrel`
        // would launch the GUI, print nothing, and never exit.
        h.check("kestrel is the CLI and not the app, which share a name on a case-insensitive disk") {
            let cli = URL(fileURLWithPath: ".build/debug/kestrel")
            let app = URL(fileURLWithPath: ".build/debug/KestrelApp")

            let files = FileManager.default
            try expect(files.fileExists(atPath: cli.path), "the CLI binary should be built")

            if files.fileExists(atPath: app.path) {
                let cliID = try files.attributesOfItem(atPath: cli.path)[.systemFileNumber] as? Int
                let appID = try files.attributesOfItem(atPath: app.path)[.systemFileNumber] as? Int
                try expect(
                    cliID != appID,
                    "kestrel and KestrelApp are the same file (inode \(cliID ?? -1)): "
                        + "a product named the same as the app up to case overwrites it"
                )
            }
        }

        h.check("help goes to standard output and succeeds") {
            let result = kestrel(["help"])
            try expectEqual(result.status, 0, "exit code")
            try expect(result.out.contains("kestrel <command>"), "should print usage")
            try expect(result.err.isEmpty, "nothing on stderr: \(result.err)")
        }

        h.check("no arguments is a usage error, so a script notices") {
            let result = kestrel([])
            try expectEqual(result.status, 2, "exit code")
        }

        h.check("an unknown command exits 2 and names the command") {
            let result = kestrel(["frobnicate"])
            try expectEqual(result.status, 2, "exit code")
            try expect(result.err.contains("frobnicate"), "should name it: \(result.err)")
        }

        h.check("a misspelled option is refused rather than ignored") {
            let result = kestrelLocal(["topics", "--parition", "3"])
            try expectEqual(result.status, 2, "exit code")
            try expect(result.err.contains("--parition"), "should name it: \(result.err)")
        }

        h.check("a command's help lists that command's options") {
            let result = kestrel(["consume", "--help"])
            try expectEqual(result.status, 0, "exit code")
            try expect(result.out.contains("--partition"), "should list --partition")
            try expect(result.out.contains("--tail"), "should list --tail")
        }
    }

    harness.suite("Choosing a cluster") { h in
        h.check("clusters lists the saved profiles without needing a broker") {
            let result = kestrel(["clusters"])
            try expectEqual(result.status, 0, "exit code")
            try expect(result.out.contains("local"), "should list the local profile: \(result.out)")
        }

        h.check("an unknown cluster name lists the ones that exist") {
            let result = kestrel(["topics", "--cluster", "nowhere"])
            try expectEqual(result.status, 2, "exit code")
            try expect(result.err.contains("nowhere"), "should name what was asked for")
            try expect(result.err.contains("local"), "should list what exists: \(result.err)")
        }

        h.check("a cluster name matches whatever its case") {
            let result = kestrel(["brokers", "--cluster", "LOCAL"])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
        }

        h.check("--bootstrap works with nothing saved") {
            let result = kestrel(["brokers", "--bootstrap", "localhost:19092"])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.out.contains("19092"), "should list the broker: \(result.out)")
        }

        h.check("--cluster and --bootstrap together are refused") {
            let result = kestrel(["topics", "--cluster", "local", "--bootstrap", "x:1"])
            try expectEqual(result.status, 2, "exit code")
            try expect(result.err.contains("not both"), "should say so: \(result.err)")
        }

        h.check("neither --cluster nor --bootstrap is a usage error") {
            let result = kestrel(["topics"])
            try expectEqual(result.status, 2, "exit code")
            try expect(result.err.contains("--cluster"), "should ask for one: \(result.err)")
        }

        // The Keychain asks the user before letting a different binary read a
        // secret the app wrote, and in a script that prompt cannot be
        // answered. `--no-keychain` is the way out, so it has to work.
        h.check("--no-keychain still runs against a plaintext cluster") {
            let result = kestrelLocal(["brokers", "--no-keychain"])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.out.contains("19092"), "should still list the broker")
        }

        h.check("--compression sends the record, and a misspelled codec is refused") {
            let sent = kestrelLocal([
                "produce", "kestrel.produce.test",
                "--value", "compressed from the CLI",
                "--compression", "gzip"
            ])
            try expectEqual(sent.status, 0, "exit code: \(sent.err)")

            // Exit 2, not 1: a codec that does not exist is a typo in the
            // command line, not a broker refusing the record.
            let typo = kestrelLocal([
                "produce", "kestrel.produce.test",
                "--value", "x",
                "--compression", "snapy"
            ])
            try expectEqual(typo.status, 2, "exit code")
            try expect(typo.err.contains("snapy"), "should quote what was typed: \(typo.err)")
            try expect(typo.err.contains("snappy"), "should list the real codecs: \(typo.err)")
        }
    }

    await harness.asyncSuite("Listing through the CLI") { h in
        h.check("topics hides internal topics unless asked") {
            let plain = kestrelLocal(["topics"])
            let all = kestrelLocal(["topics", "--internal"])
            try expectEqual(plain.status, 0, "exit code: \(plain.err)")
            try expect(
                !plain.out.contains("__consumer_offsets"),
                "should hide internal topics"
            )
            try expect(
                all.out.contains("__consumer_offsets"),
                "--internal should show them: \(all.out)"
            )
        }

        // This is the slice's acceptance line. The GUI's own list is printed by
        // the app's `dump-topics` snapshot action, so the two are compared
        // rather than described.
        await h.checkAsync("topics lists exactly what the app's sidebar lists") {
            let cli = kestrelLocal(["topics"])
            try expectEqual(cli.status, 0, "exit code: \(cli.err)")

            let fromCLI = Set(
                cli.rows.compactMap { row -> String? in
                    row.split(separator: " ").first.map(String.init)
                }
            )

            let profile = try require(
                try ClusterProfileRepository().load()
                    .first { $0.name == "local" },
                "the local profile"
            )
            let client = try KafkaClient(profile: profile)
            let metadata = try await client.metadata()
            // The sidebar hides internal topics, and so does the CLI.
            let fromBroker = Set(
                metadata.topics.filter { !$0.isInternal }.map(\.name)
            )

            try expectEqual(fromCLI, fromBroker, "the CLI's topic list")
        }

        h.check("a topic's detail lists its partitions with leaders and ISR") {
            let result = kestrelLocal(["topics", "kestrel.export.multi"])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expectEqual(result.rows.count, 3, "partition rows")
            try expect(result.out.contains("In Sync"), "should have an ISR column")
        }

        h.check("a topic that is not there is named, and hints at near misses") {
            let result = kestrelLocal(["topics", "kestrel.export.mutli"])
            try expectEqual(result.status, 1, "exit code")
            try expect(result.err.contains("kestrel.export.mutli"), "should name it")
            try expect(
                result.err.contains("kestrel.export.multi"),
                "should suggest the real one: \(result.err)"
            )
        }

        h.check("a name with nothing like it gets no misleading suggestion") {
            let result = kestrelLocal(["consume", "zzz-nothing-like-this"])
            try expectEqual(result.status, 1, "exit code")
            try expect(
                !result.err.contains("Did you mean"),
                "should not guess: \(result.err)"
            )
        }

        h.check("brokers lists the cluster's brokers") {
            let result = kestrelLocal(["brokers"])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.out.contains("localhost"), "should list the host")
        }

        h.check("groups lists consumer groups, and one group lists its lag") {
            let list = kestrelLocal(["groups"])
            try expectEqual(list.status, 0, "exit code: \(list.err)")
            try expect(
                list.out.contains(fixtureGroup),
                "should list the fixture group: \(list.out)"
            )

            let detail = kestrelLocal(["groups", fixtureGroup])
            try expectEqual(detail.status, 0, "exit code: \(detail.err)")
            try expect(detail.out.contains("Lag"), "should have a lag column")
            try expect(detail.out.contains("total lag"), "should total the lag")
        }

        h.check("a group that does not exist is reported, not shown empty") {
            let result = kestrelLocal(["groups", "kestrel.no.such.group"])
            try expectEqual(result.status, 1, "exit code")
            try expect(result.err.contains("kestrel.no.such.group"), "should name it")
        }
    }

    harness.suite("Writing and reading through the CLI") { h in
        h.check("create is idempotent, so a script can run twice") {
            let first = kestrelLocal(["topics", "create", cliTopic, "--partitions", "2"])
            try expectEqual(first.status, 0, "exit code: \(first.err)")

            let again = kestrelLocal(["topics", "create", cliTopic, "--partitions", "2"])
            try expectEqual(again.status, 0, "exit code on the second run: \(again.err)")
            try expect(
                again.out.contains("already exists"),
                "should say it was left alone: \(again.out)"
            )
        }

        h.check("produce reports the partition and offset it was given") {
            let result = kestrelLocal([
                "produce", cliTopic, "--partition", "0",
                "--key", "check-key", "--value", #"{"check":true}"#,
                "--header", "source=checks,slice=17"
            ])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.out.contains("Offset"), "should report an offset: \(result.out)")
        }

        h.check("consume reads back the key, value and headers that were sent") {
            let result = kestrelLocal(["consume", cliTopic, "--partition", "0", "--tail", "1"])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.out.contains("check-key"), "the key: \(result.out)")
            try expect(result.out.contains(#"{"check":true}"#), "the value")
            try expect(result.out.contains("source"), "the header name")
        }

        h.check("a tombstone sends a null value, and reads back as null") {
            let sent = kestrelLocal([
                "produce", cliTopic, "--partition", "0", "--key", "tomb", "--tombstone"
            ])
            try expectEqual(sent.status, 0, "exit code: \(sent.err)")

            let read = kestrelLocal(["consume", cliTopic, "--partition", "0", "--tail", "1"])
            try expect(read.out.contains("value: null"), "should be null: \(read.out)")
        }

        h.check("--value and --tombstone together are refused") {
            let result = kestrelLocal([
                "produce", cliTopic, "--value", "x", "--tombstone"
            ])
            try expectEqual(result.status, 2, "exit code")
        }

        h.check("producing needs the topic to exist, and says which one did not") {
            let result = kestrelLocal(["produce", "kestrel.cli.absent", "--value", "x"])
            try expectEqual(result.status, 1, "exit code")
            try expect(result.err.contains("kestrel.cli.absent"), "should name it: \(result.err)")
        }

        h.check("a partition the topic does not have is refused with its range") {
            let result = kestrelLocal(["consume", cliTopic, "--partition", "9"])
            try expectEqual(result.status, 1, "exit code")
            try expect(result.err.contains("0…1"), "should give the range: \(result.err)")
        }

        h.check("--offset and --tail together are refused") {
            let result = kestrelLocal([
                "consume", cliTopic, "--offset", "0", "--tail", "1"
            ])
            try expectEqual(result.status, 2, "exit code")
        }

        h.check("a header without an = is refused") {
            let result = kestrelLocal([
                "produce", cliTopic, "--value", "x", "--header", "nope"
            ])
            try expectEqual(result.status, 2, "exit code")
            try expect(result.err.contains("name=value"), "should say the shape: \(result.err)")
        }
    }

    harness.suite("The CLI's JSON output") { h in
        h.check("consume --json writes one envelope per line, which decodes") {
            let result = kestrelLocal([
                "consume", cliTopic, "--partition", "0", "--limit", "2", "--json"
            ])
            try expectEqual(result.status, 0, "exit code: \(result.err)")

            let lines = result.out.split(separator: "\n").map(String.init)
            try expect(!lines.isEmpty, "should print something")
            for line in lines {
                let envelope = try JSONDecoder().decode(
                    RecordEnvelope.self,
                    from: Data(line.utf8)
                )
                try expectEqual(envelope.topic, cliTopic, "the envelope's topic")
            }
        }

        h.check("a table's JSON form has one object per row") {
            let result = kestrelLocal(["brokers", "--json"])
            try expectEqual(result.status, 0, "exit code: \(result.err)")

            let parsed = try JSONSerialization.jsonObject(with: Data(result.out.utf8))
            let rows = try require(parsed as? [[String: Any]], "an array of objects")
            try expect(!rows.isEmpty, "should have a broker")
            try expect(rows[0]["host"] != nil, "should key by column: \(rows[0])")
        }

        h.check("notes meant for a person are left out of --json") {
            let text = kestrelLocal(["groups", fixtureGroup])
            let json = kestrelLocal(["groups", fixtureGroup, "--json"])
            try expect(text.out.contains("total lag"), "the text form totals the lag")
            try expect(
                !json.out.contains("total lag"),
                "the JSON form should not: \(json.out)"
            )
        }

        h.check("a value with quotes and newlines survives the JSON form") {
            let awkward = "line one\nline \"two\"\ttabbed"
            let sent = kestrelLocal([
                "produce", cliTopic, "--partition", "1", "--value", awkward
            ])
            try expectEqual(sent.status, 0, "exit code: \(sent.err)")

            let read = kestrelLocal([
                "consume", cliTopic, "--partition", "1", "--tail", "1", "--json"
            ])
            let line = try require(
                read.out.split(separator: "\n").last.map(String.init),
                "a line of output"
            )
            let envelope = try JSONDecoder().decode(RecordEnvelope.self, from: Data(line.utf8))
            let bytes = try require(envelope.value?.bytes, "the value's bytes")
            try expectEqual(
                String(data: bytes, encoding: .utf8),
                awkward,
                "the value, through JSON and back"
            )
        }
    }

    harness.suite("The CLI's tools") { h in
        h.check("generate produces the count it was asked for") {
            let result = kestrelLocal([
                "generate", cliTopic, "--count", "20", "--partition", "1",
                "--key", "k-{{index}}", "--value", #"{"i":{{index}},"id":"{{uuid}}"}"#,
                "--seed", "17"
            ])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.out.contains("20"), "should report 20 produced: \(result.out)")
        }

        h.check("find locates a record the generator planted, and gives its offset") {
            let result = kestrelLocal(["find", #""i":13"#, "--topic", cliTopic])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.out.contains("hit(s)"), "should count hits")
            try expect(result.rows.count >= 2, "should find at least one: \(result.out)")
        }

        h.check("find with a regex matches what a plain string would not") {
            let result = kestrelLocal([
                "find", #""i":1[0-9],"#, "--topic", cliTopic, "--regex"
            ])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.rows.count >= 2, "should match the teens: \(result.out)")
        }

        h.check("a regex that will not compile is refused before any reading") {
            let result = kestrelLocal(["find", "[unclosed", "--topic", cliTopic, "--regex"])
            try expectEqual(result.status, 1, "exit code")
            try expect(result.err.count > 10, "should explain: \(result.err)")
        }

        h.check("--keys-only and --values-only together are refused") {
            let result = kestrelLocal(["find", "x", "--keys-only", "--values-only"])
            try expectEqual(result.status, 2, "exit code")
        }

        h.check("export to a file then import into another topic keeps every record") {
            let file = FileManager.default.temporaryDirectory
                .appendingPathComponent("kestrel-cli-\(UUID().uuidString).jsonl")
            defer { try? FileManager.default.removeItem(at: file) }

            let exported = kestrelLocal([
                "export", cliTopic, "--partition", "1", "--file", file.path
            ])
            try expectEqual(exported.status, 0, "export exit code: \(exported.err)")

            let lines = try String(contentsOf: file, encoding: .utf8)
                .split(separator: "\n", omittingEmptySubsequences: true)
            try expect(lines.count >= 20, "should have written the records: \(lines.count)")

            _ = kestrelLocal(["topics", "create", cliRoundTripTopic])
            let imported = kestrelLocal([
                "import", file.path, "--topic", cliRoundTripTopic
            ])
            try expectEqual(imported.status, 0, "import exit code: \(imported.err)")
            try expect(
                imported.out.contains("\(lines.count)"),
                "should produce every line: \(imported.out)"
            )
        }

        h.check("export needs somewhere to put the records") {
            let result = kestrelLocal(["export", cliTopic])
            try expectEqual(result.status, 2, "exit code")
            try expect(result.err.contains("--file"), "should offer the options: \(result.err)")
        }

        h.check("importing a file that is not there fails without touching the topic") {
            let result = kestrelLocal([
                "import", "/tmp/kestrel-no-such-file.jsonl", "--topic", cliTopic
            ])
            try expect(result.status != 0, "should fail")
        }
    }

    await harness.asyncSuite("Avro and Connect through the CLI") { h in
        h.check("consume decodes an Avro value to JSON, as the app's detail pane does") {
            let result = kestrelLocal(["consume", avroTopic, "--tail", "1"])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.out.contains("Avro, schema id"), "should say it decoded Avro")
            try expect(!result.out.contains("base64:"), "should not fall back: \(result.out)")
        }

        h.check("produce --subject encodes JSON as Avro, and reads back decoded") {
            let sent = kestrelLocal([
                "produce", avroTopic, "--subject", avroSubject,
                "--value",
                #"{"id":"cli-check","amount":7.25,"quantity":2,"status":"SHIPPED","#
                    + #""tags":["checks"],"note":"by the checks","created":1757800000000}"#
            ])
            try expectEqual(sent.status, 0, "exit code: \(sent.err)")

            let read = kestrelLocal(["consume", avroTopic, "--tail", "1"])
            try expect(read.out.contains("cli-check"), "should read back the id: \(read.out)")
            try expect(read.out.contains("SHIPPED"), "should read back the enum")
        }

        h.check("a value that does not fit the schema names the field") {
            let result = kestrelLocal([
                "produce", avroTopic, "--subject", avroSubject, "--value", #"{"id":42}"#
            ])
            try expectEqual(result.status, 1, "exit code")
            try expect(result.err.contains("value.id"), "should name the field: \(result.err)")
            try expect(
                result.err.contains("is not a string"),
                "and read as a sentence: \(result.err)"
            )
        }

        h.check("schema lists the registry's subjects, and fetches one") {
            let subjects = kestrelLocal(["schema"])
            try expectEqual(subjects.status, 0, "exit code: \(subjects.err)")
            try expect(subjects.out.contains(avroSubject), "should list the subject")

            let one = kestrelLocal(["schema", avroSubject])
            try expectEqual(one.status, 0, "exit code: \(one.err)")
            try expect(one.out.contains("\"type\""), "should print the schema: \(one.out)")
        }

        h.check("a cluster with no registry says so, in words a terminal can act on") {
            let result = kestrel(["schema", "--bootstrap", "localhost:19092"])
            try expectEqual(result.status, 1, "exit code")
            try expect(
                result.err.contains("Schema Registry"),
                "should name the setting: \(result.err)"
            )
        }

        h.check("connect lists the connectors with their state and task counts") {
            let result = kestrelLocal(["connect"])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.out.contains("State"), "should have a state column")
            try expect(result.out.contains(connectSource), "should list the source connector")
        }

        h.check("connect status shows a failed task's trace") {
            let result = kestrelLocal(["connect", "status", connectBrokenSink])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.out.contains("FAILED"), "the sink's task fails: \(result.out)")
            try expect(result.out.contains("ConnectException"), "should show the trace")
        }

        h.check("connect config prints the connector's configuration") {
            let result = kestrelLocal(["connect", "config", connectSource])
            try expectEqual(result.status, 0, "exit code: \(result.err)")
            try expect(result.out.contains("connector.class"), "should list the class key")
        }

        h.check("a connector that is not there is reported") {
            let result = kestrelLocal(["connect", "status", "no-such-connector"])
            try expectEqual(result.status, 1, "exit code")
        }

        h.check("an unknown connect action lists the ones that exist") {
            let result = kestrelLocal(["connect", "frobnicate"])
            try expectEqual(result.status, 2, "exit code")
            try expect(result.err.contains("pause"), "should list the actions: \(result.err)")
        }

        await h.checkAsync("pause and resume through the CLI move the connector's state") {
            let paused = kestrelLocal(["connect", "pause", connectSource])
            try expectEqual(paused.status, 0, "exit code: \(paused.err)")
            let client = try KafkaConnectClient(url: connectURL)
            _ = try await awaitConnector(connectSource, on: client, state: .paused)

            let resumed = kestrelLocal(["connect", "resume", connectSource])
            try expectEqual(resumed.status, 0, "exit code: \(resumed.err)")
            _ = try await awaitConnector(connectSource, on: client, state: .running)
        }

        h.check("a cluster with no Connect URL says so") {
            let result = kestrel(["connect", "--bootstrap", "localhost:19092"])
            try expectEqual(result.status, 1, "exit code")
            try expect(result.err.contains("Connect"), "should name the setting: \(result.err)")
        }
    }
}
