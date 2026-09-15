import AppKit
import SwiftUI

/// Self-verification hook for headless checks.
///
/// macOS denies cross-process window titles and `screencapture` to this repo's
/// terminal, so the app captures its own window instead. Set `KESTREL_SNAPSHOT`
/// to a file path: shortly after launch Kestrel writes a PNG of its key window
/// there, prints the window title to stdout, and exits.
///
/// The hook is inert when the variable is unset, so release builds are unaffected.
@MainActor
enum Snapshot {
    private static var requestedPath: String? {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT"]
    }

    static var isRequested: Bool { requestedPath != nil }

    /// Steps to run before capturing, from `KESTREL_SNAPSHOT_ACTIONS`.
    ///
    /// The window cannot be driven from outside the process — this Mac denies
    /// the app assistive access — so a capture that needs UI state asks for it
    /// here. Recognised values are listed in `RootView`.
    static var requestedActions: [String] {
        (ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_ACTIONS"] ?? "")
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
    }

    /// File the import sheet should load without asking, from
    /// `KESTREL_SNAPSHOT_IMPORT_FILE`.
    ///
    /// The sheet's file picker is a modal `NSOpenPanel`, which a headless run
    /// cannot answer, so the snapshot supplies the path instead. Everything
    /// after the picker is the same code a person drives.
    static var importFile: URL? {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_IMPORT_FILE"]
            .map { URL(fileURLWithPath: $0) }
    }

    /// Topic the snapshot imports into, from `KESTREL_SNAPSHOT_IMPORT_TOPIC`.
    static var importTopic: String? {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_IMPORT_TOPIC"]
    }

    /// Whether the snapshot should press Import itself.
    static var runsImport: Bool {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_IMPORT_RUN"] == "1"
    }

    /// File the export sheet writes to without asking, from
    /// `KESTREL_SNAPSHOT_EXPORT_FILE`. Stands in for the save panel.
    static var exportFile: URL? {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_EXPORT_FILE"]
            .map { URL(fileURLWithPath: $0) }
    }

    /// Topic the snapshot exports from, from `KESTREL_SNAPSHOT_EXPORT_TOPIC`.
    static var exportTopic: String? {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_EXPORT_TOPIC"]
    }

    /// Offset range for the snapshot export, from `KESTREL_SNAPSHOT_EXPORT_RANGE`
    /// as `start-end`. Either side may be empty to leave it unbounded.
    static var exportRange: (start: String, end: String)? {
        guard let raw = ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_EXPORT_RANGE"] else {
            return nil
        }
        let parts = raw.split(separator: "-", maxSplits: 1, omittingEmptySubsequences: false)
        return (String(parts.first ?? ""), parts.count > 1 ? String(parts[1]) : "")
    }

    /// Whether the snapshot should press Export itself.
    static var runsExport: Bool {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_EXPORT_RUN"] == "1"
    }

    /// Destination topic for the snapshot export, from
    /// `KESTREL_SNAPSHOT_EXPORT_TO_TOPIC`. Writes to a file when unset.
    static var exportToTopic: String? {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_EXPORT_TO_TOPIC"]
    }

    /// Topic the snapshot generates into, from `KESTREL_SNAPSHOT_GENERATE_TOPIC`.
    static var generateTopic: String? {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_GENERATE_TOPIC"]
    }

    /// How many records the snapshot generates, from
    /// `KESTREL_SNAPSHOT_GENERATE_COUNT`.
    static var generateCount: String? {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_GENERATE_COUNT"]
    }

    /// Whether the snapshot should press Generate itself.
    static var runsGenerate: Bool {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_GENERATE_RUN"] == "1"
    }

    /// Text the find sheet searches for, from `KESTREL_SNAPSHOT_FIND`.
    static var findQuery: String? {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_FIND"]
    }

    /// Whether the snapshot searches the whole cluster rather than one topic,
    /// from `KESTREL_SNAPSHOT_FIND_CLUSTER=1`.
    static var findScopeIsCluster: Bool {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_FIND_CLUSTER"] == "1"
    }

    /// Whether the snapshot should open the first hit, from
    /// `KESTREL_SNAPSHOT_FIND_OPEN=1`. Stands in for double-clicking a result.
    static var opensFirstHit: Bool {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_FIND_OPEN"] == "1"
    }

    /// Whether the snapshot should press Find itself.
    static var runsFind: Bool {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_FIND_RUN"] == "1"
    }

    /// Opens the produce sheet with the value format set to Avro, from
    /// `KESTREL_SNAPSHOT_PRODUCE_AVRO=1`. The picker cannot be clicked from
    /// outside the process, so the state has to be asked for.
    static var producesAvro: Bool {
        ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_PRODUCE_AVRO"] == "1"
    }

    /// Seconds to wait for layout to settle, from `KESTREL_SNAPSHOT_DELAY`.
    ///
    /// Heavier screens (tables, a config fetch) need longer than a plain form;
    /// too short a wait captures an uncomposited, blank window.
    private static var delay: Duration {
        let seconds = ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_DELAY"]
            .flatMap(Double.init) ?? 2
        return .milliseconds(Int(seconds * 1000))
    }

    /// Captures the key window once layout has settled, then terminates the app.
    ///
    /// Do not activate or reorder the window first: that races its first
    /// display and the window server hands back a fully transparent buffer at
    /// an intermediate size. Waiting is enough.
    static func runIfRequested() async {
        guard let path = requestedPath else { return }
        try? await Task.sleep(for: delay)

        if ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_DEBUG"] == "1" {
            for candidate in NSApp.windows {
                print("""
                    SNAPSHOT_WINDOW title=\(candidate.title.isEmpty ? "<none>" : candidate.title) \
                    number=\(candidate.windowNumber) visible=\(candidate.isVisible) \
                    onScreen=\(candidate.isOnActiveSpace) frame=\(candidate.frame) \
                    contentSize=\(candidate.contentView?.bounds.size.debugDescription ?? "nil")
                    """)
            }
        }

        // A sheet is its own window, so capturing the parent yields only the
        // dimmed content behind it. Prefer the sheet when one is up, which is
        // what a snapshot of a sheet is asking for.
        let candidates = NSApp.windows.filter { $0.isVisible && $0.contentView != nil }
        guard let window = candidates.compactMap(\.attachedSheet).first ?? candidates.first,
              let view = window.contentView
        else {
            print("SNAPSHOT_FAILED no visible window")
            exit(1)
        }

        // Panes further down a scroll view are off screen at the default size,
        // so a capture can ask for a taller window and wait for it to lay out.
        if let height = ProcessInfo.processInfo.environment["KESTREL_SNAPSHOT_HEIGHT"]
            .flatMap(Double.init) {
            var frame = window.frame
            frame.origin.y -= height - frame.height
            frame.size.height = height
            window.setFrame(frame, display: true)
            try? await Task.sleep(for: .seconds(1))
        }

        guard let png = capture(window: window, view: view) else {
            print("SNAPSHOT_FAILED render")
            exit(1)
        }

        do {
            try png.write(to: URL(fileURLWithPath: path))
            print("SNAPSHOT_TITLE=\(window.title)")
            print("SNAPSHOT_SIZE=\(Int(view.bounds.width))x\(Int(view.bounds.height))")
            print("SNAPSHOT_PATH=\(path)")
            exit(0)
        } catch {
            print("SNAPSHOT_FAILED \(error.localizedDescription)")
            exit(1)
        }
    }

    /// Captures the window as PNG data, trying each path until one yields pixels.
    ///
    /// The window server reproduces the window exactly, but it depends on the
    /// Screen Recording permission and, when that is missing, returns a fully
    /// transparent buffer instead of failing — so every result is checked for
    /// content rather than trusted. The PDF path re-draws the view hierarchy and
    /// needs no permission; `cacheDisplay` is the last resort and drops most text.
    private static func capture(window: NSWindow, view: NSView) -> Data? {
        let attempts: [(String, () -> Data?)] = [
            ("windowServer", { captureViaWindowServer(window) }),
            ("pdf", { captureViaPDF(view) }),
            ("cacheDisplay", { captureViaCacheDisplay(view) })
        ]

        for (name, attempt) in attempts {
            guard let data = attempt(), hasVisibleContent(data) else { continue }
            print("SNAPSHOT_MODE=\(name)")
            return data
        }
        return nil
    }

    /// True when the PNG has at least some opaque, non-uniform pixels.
    ///
    /// A denied Screen Recording permission produces a plausible-looking image
    /// that is entirely transparent, which this rejects.
    private static func hasVisibleContent(_ png: Data) -> Bool {
        guard let rep = NSBitmapImageRep(data: png), let image = rep.cgImage else { return false }

        let width = 32
        let height = 32
        var pixels = [UInt8](repeating: 0, count: width * height * 4)
        guard let context = CGContext(
            data: &pixels,
            width: width,
            height: height,
            bitsPerComponent: 8,
            bytesPerRow: width * 4,
            space: CGColorSpace(name: CGColorSpace.sRGB)!,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        ) else { return false }

        context.draw(image, in: CGRect(x: 0, y: 0, width: width, height: height))

        let opaque = stride(from: 3, to: pixels.count, by: 4).filter { pixels[$0] > 8 }
        return opaque.count > (width * height) / 2
    }

    private static func captureViaWindowServer(_ window: NSWindow) -> Data? {
        let id = CGWindowID(window.windowNumber)
        guard window.windowNumber > 0,
              let image = CGWindowListCreateImage(
                  .null,
                  .optionIncludingWindow,
                  id,
                  [.boundsIgnoreFraming, .bestResolution]
              ),
              image.width > 1, image.height > 1
        else { return nil }

        return NSBitmapImageRep(cgImage: image).representation(using: .png, properties: [:])
    }

    /// Re-draws the view hierarchy through AppKit's PDF context.
    private static func captureViaPDF(_ view: NSView) -> Data? {
        let pdf = view.dataWithPDF(inside: view.bounds)
        guard let page = NSPDFImageRep(data: pdf) else { return nil }

        let scale: CGFloat = 2
        let size = view.bounds.size
        guard let rep = NSBitmapImageRep(
            bitmapDataPlanes: nil,
            pixelsWide: Int(size.width * scale),
            pixelsHigh: Int(size.height * scale),
            bitsPerSample: 8,
            samplesPerPixel: 4,
            hasAlpha: true,
            isPlanar: false,
            colorSpaceName: .deviceRGB,
            bytesPerRow: 0,
            bitsPerPixel: 0
        ) else { return nil }
        rep.size = size

        NSGraphicsContext.saveGraphicsState()
        defer { NSGraphicsContext.restoreGraphicsState() }
        NSGraphicsContext.current = NSGraphicsContext(bitmapImageRep: rep)
        NSColor.windowBackgroundColor.setFill()
        NSRect(origin: .zero, size: size).fill()
        page.draw(in: NSRect(origin: .zero, size: size))

        return rep.representation(using: .png, properties: [:])
    }

    private static func captureViaCacheDisplay(_ view: NSView) -> Data? {
        guard let rep = view.bitmapImageRepForCachingDisplay(in: view.bounds) else { return nil }
        view.cacheDisplay(in: view.bounds, to: rep)
        return rep.representation(using: .png, properties: [:])
    }
}
