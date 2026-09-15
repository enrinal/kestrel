import Foundation
import KestrelKit

/// `kestrel`, the command-line half of the app.
///
/// Every command goes through the same KestrelKit types the GUI uses, and
/// `--cluster` reads the profiles the GUI saves, so the two cannot disagree
/// about what a cluster is or what is on it.
///
/// Exit codes: 0 success, 1 the command failed, 2 the command line was wrong.
/// A script can tell a typo from a broker refusing without reading messages.
enum ExitCode: Int32 {
    case success = 0
    case failure = 1
    case usage = 2
}

let usage = """
    kestrel — a command-line Kafka explorer

    USAGE
      kestrel <command> [arguments] [--cluster <name> | --bootstrap <host:port>]

    COMMANDS
      clusters                      list the saved cluster profiles
      brokers                       list the cluster's brokers
      topics [<topic>]              list topics, or describe one's partitions
      topics create|delete <topic>  add or remove a topic
      consume <topic>               read records from a partition
      produce <topic>               send one record
      groups [<group>]              list consumer groups, or describe one's offsets
      schema [<subject>]            list registry subjects, or fetch a schema
      connect [<action>] [<name>]   list connectors, or status/config/pause/resume/restart
      import <file>                 produce the records in a file
      export <topic>                write a topic's records to a file or another topic
      generate <topic>              produce records from templates
      find <text>                   search keys and values across topics

    CLUSTER
      --cluster <name>              a cluster saved by the app (see `kestrel clusters`)
      --bootstrap <host:port>       a plaintext cluster, with nothing saved
      --timeout <seconds>           per-request timeout, default 10
      --compression <codec>         none, gzip, snappy, lz4 or zstd. Applies to the
                                    commands that produce, for this run only; the
                                    saved profile's codec is the default. Reading a
                                    compressed topic needs no setting at all.
      --no-keychain                 skip the saved secrets, and the prompt reading them
                                    causes. Needed in scripts and over ssh, where the
                                    prompt cannot be answered.

    OUTPUT
      --json                        machine-readable output
      --help                        this text, or a command's options

    EXAMPLES
      kestrel topics --cluster local
      kestrel consume orders --cluster local --partition 0 --tail 5
      kestrel produce orders --cluster local --key k1 --value '{"id":1}'
      kestrel produce orders --cluster local --value '{"id":1}' --compression snappy
      kestrel groups my-group --cluster local
      kestrel find needle --cluster local --topic orders --regex
      kestrel connect status my-connector --cluster local
    """

/// Per-command help, printed for `kestrel <command> --help`.
let commandHelp: [String: String] = [
    "clusters": "kestrel clusters\n  Lists the profiles saved in the app's clusters.json.",
    "brokers": "kestrel brokers --cluster <name>",
    "topics": """
        kestrel topics [<topic>] --cluster <name>
          --internal        include internal topics such as __consumer_offsets
        kestrel topics create <topic> --cluster <name>
          --partitions <n>  default 1
          --replication <n> default 1
          --config <k=v,..> topic configs, comma separated
        kestrel topics delete <topic> --cluster <name>
        """,
    "consume": """
        kestrel consume <topic> --cluster <name>
          --partition <n>   partition to read, default 0
          --limit <n>       how many records, default 10
          --offset <n>      start at this offset
          --tail <n>        start n records before the end
          --no-headers      omit headers from the output
          --json            one JSON envelope per line, which `import` reads back
        """,
    "produce": """
        kestrel produce <topic> --cluster <name> --value <text>
          --key <text>      record key
          --value <text>    record value, or JSON to encode when --subject is given
          --subject <name>  encode the value as Avro against this subject's newest schema
          --partition <n>   send to a specific partition
          --header <k=v,..> headers, comma separated
          --tombstone       send a null value instead of --value
          --compression <c> none, gzip, snappy, lz4 or zstd, default the profile's
        """,
    "groups": "kestrel groups [<group>] --cluster <name>",
    "schema": """
        kestrel schema [<subject>] --cluster <name>
          --id <n>          fetch by schema id instead of subject
        """,
    "connect": """
        kestrel connect [list|status|config|pause|resume|restart] [<connector>] --cluster <name>
        """,
    "import": """
        kestrel import <file> --cluster <name> --topic <name>
          Reads saved envelopes or plain JSON lines, the same formats the app imports.
          --compression <c> none, gzip, snappy, lz4 or zstd, default the profile's
        """,
    "export": """
        kestrel export <topic> --cluster <name> [--file <path> | --to-topic <name>]
          --partition <n>   one partition, default all
          --from <offset>   first offset to include
          --to <offset>     one past the last offset to include
          --compression <c> codec for --to-topic, default the profile's
        """,
    "generate": """
        kestrel generate <topic> --cluster <name>
          --count <n>       how many records, default 10
          --key <template>  key template
          --value <template> value template, default {"index":{{index}},"id":"{{uuid}}"}
          --partition <n>   send every record to one partition
          --seed <n>        seed the randomness, for reproducible output
          --compression <c> none, gzip, snappy, lz4 or zstd, default the profile's
          Placeholders: {{index}} {{uuid}} {{timestamp}} {{millis}} {{lorem}} {{int:a-b}} {{choice:x|y}}
        """,
    "find": """
        kestrel find <text> --cluster <name>
          --topic <a,b>     topics to search, default every non-internal topic
          --regex           treat the text as a regular expression
          --case-sensitive  match case, which is off by default
          --keys-only       search keys only
          --values-only     search values only
          --limit <n>       stop after this many hits, default 100
        """
]

/// Commands that work without a cluster, and so must not demand one.
let clusterlessCommands: Set<String> = ["clusters", "help"]

func run() async -> ExitCode {
    var arguments: Arguments
    do {
        arguments = try Arguments(Array(CommandLine.arguments.dropFirst()))
    } catch let error as UsageError {
        printError(error.description)
        return .usage
    } catch {
        printError(error.localizedDescription)
        return .usage
    }

    guard let command = arguments.command, command != "help" else {
        // Help on stdout: it is what was asked for, not an error.
        print(usage)
        return arguments.command == nil && !arguments.wantsHelp ? .usage : .success
    }

    if arguments.wantsHelp {
        print(commandHelp[command] ?? usage)
        return .success
    }

    do {
        if clusterlessCommands.contains(command) {
            switch command {
            case "clusters": try Commands.clusters(&arguments)
            default: break
            }
            return .success
        }

        let (profile, secrets) = try ClusterResolver.resolve(&arguments)
        let seconds = try arguments.int("timeout") ?? 10
        let context = Context(
            profile: profile,
            secrets: secrets,
            output: Output(wantsJSON: arguments.wantsJSON),
            timeout: .seconds(seconds)
        )

        switch command {
        case "brokers": try await Commands.brokers(&arguments, context)
        case "topics": try await Commands.topics(&arguments, context)
        case "consume": try await Commands.consume(&arguments, context)
        case "produce": try await Commands.produce(&arguments, context)
        case "groups": try await Commands.groups(&arguments, context)
        case "schema": try await Commands.schema(&arguments, context)
        case "connect": try await Commands.connect(&arguments, context)
        case "import": try await Commands.importRecords(&arguments, context)
        case "export": try await Commands.export(&arguments, context)
        case "generate": try await Commands.generate(&arguments, context)
        case "find": try await Commands.find(&arguments, context)
        default:
            printError("unknown command \(command). Run `kestrel help` for the list.")
            return .usage
        }
        return .success
    } catch let error as UsageError {
        printError(error.description)
        printError("run `kestrel \(command) --help` for this command's options")
        return .usage
    } catch let error as CLIFailure {
        printError(error.description)
        return .failure
    } catch {
        // LocalizedError carries the sentence worth showing; the Kafka,
        // registry and Connect errors all provide one.
        printError((error as? LocalizedError)?.errorDescription ?? error.localizedDescription)
        return .failure
    }
}

exit(await run().rawValue)
