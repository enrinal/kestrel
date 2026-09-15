import AppKit
import Foundation

/// Runs a command and returns its output, for poking at the built artifacts.
func shell(_ launchPath: String, _ arguments: [String]) -> (status: Int32, out: String) {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: launchPath)
    process.arguments = arguments
    let pipe = Pipe()
    process.standardOutput = pipe
    process.standardError = FileHandle.nullDevice

    do {
        try process.run()
    } catch {
        return (-1, "")
    }
    let data = pipe.fileHandleForReading.readDataToEndOfFile()
    process.waitUntilExit()
    return (process.terminationStatus, String(data: data, encoding: .utf8) ?? "")
}

let iconURL = URL(fileURLWithPath: "Resources/AppIcon.icns")
let bundleURL = URL(fileURLWithPath: "build/Kestrel.app")
let dmgURL = URL(fileURLWithPath: "dist/Kestrel.dmg")

@MainActor
func registerPackagingChecks(_ harness: Harness) {
    harness.suite("The app icon") { h in
        h.check("the icns is committed, so a clean checkout still has an icon") {
            try expect(
                FileManager.default.fileExists(atPath: iconURL.path),
                "Resources/AppIcon.icns should exist; run `swift Scripts/make-icon.swift`"
            )
        }

        h.check("it carries every size macOS asks for, 16 through 512 at 1x and 2x") {
            let image = try require(NSImage(contentsOf: iconURL), "a readable icns")
            let sides = Set(image.representations.map(\.pixelsWide))
            // 16@2x and 32@1x are both 32 pixels, and so on, which leaves six
            // distinct pixel sizes for the ten renditions in the iconset.
            for expected in [16, 32, 64, 128, 256, 512, 1024] {
                try expect(sides.contains(expected), "missing a \(expected)px rendition: \(sides.sorted())")
            }
        }

        // The point of the mark is that it survives being tiny. A 16px
        // rendition that is blank, or almost entirely background, would pass a
        // "file exists" check and look like nothing in the Dock.
        h.check("the 16px rendition actually has a mark on it") {
            let image = try require(NSImage(contentsOf: iconURL), "a readable icns")
            let small = try require(
                image.representations.first { $0.pixelsWide == 16 },
                "a 16px rendition"
            )
            let bitmap = try require(small as? NSBitmapImageRep, "a bitmap rendition")

            var opaque = 0
            var amber = 0
            for x in 0..<16 {
                for y in 0..<16 {
                    guard let colour = bitmap.colorAt(x: x, y: y) else { continue }
                    if colour.alphaComponent > 0.5 { opaque += 1 }
                    // The mark is warm and the plate is cold, so counting
                    // pixels where red clearly leads blue counts the mark.
                    if colour.redComponent > colour.blueComponent + 0.2 { amber += 1 }
                }
            }

            try expect(opaque > 120, "the plate should fill most of the tile, got \(opaque) of 256")
            try expect(amber > 12, "the mark should be visible at 16px, got \(amber) amber pixels")
        }

        h.check("Info.plist points at the icon and at the right executable") {
            let data = try Data(contentsOf: URL(fileURLWithPath: "Resources/Info.plist"))
            let plist = try require(
                try PropertyListSerialization.propertyList(from: data, format: nil)
                    as? [String: Any],
                "a readable Info.plist"
            )
            try expectEqual(plist["CFBundleIconFile"] as? String, "AppIcon", "CFBundleIconFile")
            // The target is KestrelApp but the file in the bundle is Kestrel;
            // if those ever disagree the app launches to a dialog about a
            // missing executable.
            try expectEqual(plist["CFBundleExecutable"] as? String, "Kestrel", "CFBundleExecutable")
        }
    }

    harness.suite("The app bundle") { h in
        let files = FileManager.default
        guard files.fileExists(atPath: bundleURL.path) else {
            h.check("skipped: build/Kestrel.app is not staged (run Scripts/build-app.sh)") {}
            return
        }

        h.check("the executable and the bundled CLI are both there") {
            try expect(
                files.fileExists(atPath: bundleURL.path + "/Contents/MacOS/Kestrel"),
                "the app executable"
            )
            // In Helpers, not MacOS: `kestrel` and `Kestrel` cannot share a
            // directory on a case-insensitive volume.
            try expect(
                files.fileExists(atPath: bundleURL.path + "/Contents/Helpers/kestrel"),
                "the CLI, in Contents/Helpers"
            )
        }

        // Without this the DMG only runs on a Mac that has already run
        // `brew install librdkafka`, which is not a thing to ask of someone who
        // just opened a disk image.
        h.check("no binary in the bundle still loads a library from Homebrew") {
            let binaries = [
                "/Contents/MacOS/Kestrel",
                "/Contents/Helpers/kestrel"
            ].map { bundleURL.path + $0 }

            let frameworks = (try? files.contentsOfDirectory(
                atPath: bundleURL.path + "/Contents/Frameworks"
            )) ?? []
            let libraries = frameworks.map {
                bundleURL.path + "/Contents/Frameworks/" + $0
            }

            for binary in binaries + libraries {
                let listed = shell("/usr/bin/otool", ["-L", binary]).out
                try expect(
                    !listed.contains("/opt/homebrew"),
                    "\(URL(fileURLWithPath: binary).lastPathComponent) still loads from Homebrew"
                )
            }
        }

        h.check("librdkafka and everything it pulls in are inside the bundle") {
            let frameworks = try files.contentsOfDirectory(
                atPath: bundleURL.path + "/Contents/Frameworks"
            )
            // librdkafka needs lz4 and zstd for compression and OpenSSL for
            // TLS and SASL, and libssl needs libcrypto.
            for library in ["librdkafka", "liblz4", "libzstd", "libssl", "libcrypto"] {
                try expect(
                    frameworks.contains { $0.hasPrefix(library) },
                    "\(library) should be bundled, have: \(frameworks.sorted())"
                )
            }
        }

        h.check("the bundle carries a signature, which macOS requires of SwiftUI apps") {
            let result = shell("/usr/bin/codesign", ["--verify", "--deep", bundleURL.path])
            try expectEqual(result.status, 0, "codesign --verify")
        }

        h.check("the CLI inside the bundle runs, and finds the saved clusters") {
            let result = shell(bundleURL.path + "/Contents/Helpers/kestrel", ["clusters"])
            try expectEqual(result.status, 0, "exit code")
            try expect(result.out.contains("local"), "should list the profile: \(result.out)")
        }
    }

    harness.suite("The disk image") { h in
        guard FileManager.default.fileExists(atPath: dmgURL.path) else {
            h.check("skipped: dist/Kestrel.dmg is not built (run Scripts/make-dmg.sh)") {}
            return
        }

        // The slice's acceptance line, checked the way it is written: mount the
        // image and look inside it.
        h.check("it mounts, and holds the app, an Applications symlink and a note") {
            let mount = shell("/usr/bin/hdiutil", [
                "attach", dmgURL.path, "-nobrowse", "-readonly", "-plist"
            ])
            try expectEqual(mount.status, 0, "hdiutil attach")

            // The mount point is in the plist hdiutil prints.
            let point = try require(
                mount.out
                    .components(separatedBy: "<string>")
                    .map { $0.components(separatedBy: "</string>")[0] }
                    .first { $0.hasPrefix("/Volumes/") },
                "a mount point in hdiutil's output"
            )
            defer { _ = shell("/usr/bin/hdiutil", ["detach", point, "-force", "-quiet"]) }

            let files = FileManager.default
            try expect(
                files.fileExists(atPath: point + "/Kestrel.app"),
                "Kestrel.app should be in the image"
            )
            try expect(
                files.fileExists(atPath: point + "/Kestrel.app/Contents/MacOS/Kestrel"),
                "and be a real bundle, not an empty folder"
            )

            let applications = try files.destinationOfSymbolicLink(atPath: point + "/Applications")
            try expectEqual(applications, "/Applications", "the symlink's target")

            // An ad-hoc signed app cannot be opened by double-clicking, so the
            // image has to say how.
            let note = try String(contentsOfFile: point + "/Read Me.txt", encoding: .utf8)
            try expect(note.contains("Right-click"), "the note should explain the first open")
        }

        h.check("it is compressed and read-only, as a distributable image should be") {
            let info = shell("/usr/bin/hdiutil", ["imageinfo", dmgURL.path]).out
            try expect(info.contains("UDZO") || info.contains("zlib"), "should be UDZO: \(info.prefix(200))")
        }
    }
}
