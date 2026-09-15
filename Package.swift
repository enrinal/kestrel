// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "Kestrel",
    platforms: [.macOS(.v14)],
    products: [
        // Declared so the binary is named `kestrel`, as the CLI is invoked, and
        // not `KestrelCLI` after its target.
        .executable(name: "kestrel", targets: ["KestrelCLI"])
    ],
    targets: [
        // Models, persistence, and (later) the Kafka client. Shared by the app
        // and by the `kestrel` CLI in slice 17.
        // librdkafka, resolved through pkg-config (`brew install librdkafka`).
        .systemLibrary(
            name: "Crdkafka",
            path: "Sources/Crdkafka",
            pkgConfig: "rdkafka",
            providers: [.brew(["librdkafka"])]
        ),
        .target(
            name: "KestrelKit",
            dependencies: ["Crdkafka"],
            path: "Sources/KestrelKit"
        ),
        // Named KestrelApp, not Kestrel, because the CLI product is `kestrel`
        // and this Mac's APFS volume is case-insensitive: two products whose
        // names differ only in case write to one file in .build, so whichever
        // linked last won and `kestrel` would silently launch the GUI.
        .executableTarget(
            name: "KestrelApp",
            dependencies: ["KestrelKit"],
            path: "Sources/KestrelApp"
        ),
        // Checks live in an executable, not a test target: this machine has
        // Command Line Tools only, which ships neither XCTest nor swift-testing,
        // so `swift test` cannot build. Run `swift run KestrelChecks`.
        // The `kestrel` command-line tool. Shares KestrelKit with the app, so
        // the two cannot disagree about a cluster or what is on it.
        .executableTarget(
            name: "KestrelCLI",
            dependencies: ["KestrelKit"],
            path: "Sources/KestrelCLI"
        ),
        .executableTarget(
            name: "KestrelChecks",
            dependencies: ["KestrelKit"],
            path: "Sources/KestrelChecks"
        )
    ]
)
