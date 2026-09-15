import Foundation

/// Reads and writes the cluster list as JSON.
///
/// The default location is
/// `~/Library/Application Support/Kestrel/clusters.json`. The file holds no
/// secrets — ``ClusterProfile`` has no field that can carry one.
public struct ClusterProfileRepository: Sendable {
    public static let fileName = "clusters.json"
    public static let directoryName = "Kestrel"

    public let fileURL: URL

    /// Creates a repository rooted at `directory`.
    ///
    /// - Parameter directory: folder holding `clusters.json`. Pass `nil` for the
    ///   app's Application Support folder; tests pass a temporary directory.
    public init(directory: URL? = nil) {
        if let directory {
            fileURL = directory.appendingPathComponent(Self.fileName)
        } else {
            let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            fileURL = base
                .appendingPathComponent(Self.directoryName, isDirectory: true)
                .appendingPathComponent(Self.fileName)
        }
    }

    /// Loads saved profiles, or an empty array when nothing has been saved yet.
    public func load() throws -> [ClusterProfile] {
        guard FileManager.default.fileExists(atPath: fileURL.path) else { return [] }
        let data = try Data(contentsOf: fileURL)
        guard !data.isEmpty else { return [] }
        return try JSONDecoder().decode([ClusterProfile].self, from: data)
    }

    /// Writes the profile list, creating the containing folder if needed.
    ///
    /// The write is atomic, so a crash mid-save cannot truncate an existing list.
    public func save(_ profiles: [ClusterProfile]) throws {
        try FileManager.default.createDirectory(
            at: fileURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        try encoder.encode(profiles).write(to: fileURL, options: .atomic)
    }
}
