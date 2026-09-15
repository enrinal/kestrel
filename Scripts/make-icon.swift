// Draws Kestrel's app icon and writes Resources/AppIcon.icns.
//
// The icon is generated rather than checked in as a binary, so it can be
// adjusted by editing numbers here and re-running:
//
//     swift Scripts/make-icon.swift
//
// The mark is a kestrel in a stoop, seen from below: swept-back pointed wings,
// a spindle body, a narrow tail. Drawn as one symmetric path so it stays a
// solid silhouette at 16pt, where any interior detail turns to mush.

import AppKit
import CoreGraphics
import Foundation

let canvas = 1024.0

/// The rounded square the glyph sits on.
///
/// Inset from the canvas because macOS expects an app icon's artwork to stop
/// short of the edges; a full-bleed square looks oversized next to every other
/// icon in the Dock.
let plateInset = 88.0
let cornerRadius = 196.0

// MARK: - The bird

func at(_ x: Double, _ y: Double) -> CGPoint { CGPoint(x: x, y: y) }

// MARK: - The mark

/// A K with two swept blades, in Kestrel's own coordinate space.
///
/// Three attempts at drawing the bird itself are worth recording, because they
/// failed for the same reason. A kestrel seen from below became a stingray; a
/// rounded head in profile became a kiwi; an angular head became a pentagon
/// with a nub. A silhouette a few pixels across keeps only its outline, and a
/// rounded outline becomes whichever animal the viewer thinks of first.
///
/// A letterform cannot be mistaken for the wrong animal, and it is legible at
/// 16 points, which is the size that actually matters in a Dock. The arms are
/// swept to points rather than cut flat at cap height, which is the nod to a
/// wing — and the one thing here that is not just a letter K.
///
/// Traced as a single closed outline so it fills with one winding rule; the
/// blades overlapping the stem would cancel each other under even-odd.
/// Each blade is a bar with parallel edges, ending in a vertical cut, and the
/// two meet at a single point on the stem — the waist.
///
/// Two things had to be got right here, and both were wrong first time. The
/// blades ended in points, which looked sharp at 1024px and vanished at 16px,
/// leaving something that read as a lower-case l. Then the blunt ends were cut
/// horizontally at cap height, which sent the tip edge back across the leading
/// edge and filled the letter in as a bowtie. Parallel edges plus a vertical
/// terminal cannot self-intersect.
let markOutline: [CGPoint] = [
    at(170, 870),   // stem, top left
    at(350, 870),   // stem, top right
    at(350, 640),   // upper blade, leading edge leaves the stem
    at(852, 870),   // upper blade, leading edge at cap height
    at(852, 730),   // upper blade terminal, cut vertically
    at(350, 500),   // the waist, where both blades meet the stem
    at(852, 270),   // lower blade, leading edge
    at(852, 130),   // lower blade terminal, at the baseline
    at(350, 360),   // lower blade, trailing edge back to the stem
    at(350, 130),   // stem, bottom right
    at(170, 130)    // stem, bottom left
]

/// Maps the mark's own coordinates onto the plate: scaled to `fill` of the
/// plate and centred on it.
///
/// Worked out from the outline rather than typed in, because centring by eye
/// left every earlier draft sitting high and to one side.
func glyphTransform(side: Double, fill: Double = 0.66) -> CGAffineTransform {
    let minX = markOutline.map(\.x).min()!
    let maxX = markOutline.map(\.x).max()!
    let minY = markOutline.map(\.y).min()!
    let maxY = markOutline.map(\.y).max()!

    let plate = side - 2 * plateInset * side / canvas
    let scale = plate * fill / max(maxX - minX, maxY - minY)

    return CGAffineTransform(translationX: side / 2, y: side / 2)
        .scaledBy(x: scale, y: scale)
        .translatedBy(x: -(minX + maxX) / 2, y: -(minY + maxY) / 2)
}

/// Builds a closed path through `points`, with the corners rounded a little so
/// the mark looks drawn rather than clipped.
func polygon(_ points: [CGPoint], radius: Double, transform: CGAffineTransform) -> CGPath {
    let path = CGMutablePath()
    guard points.count >= 3 else { return path }

    // Each corner is cut back along both of its edges and joined with an arc,
    // which is what `addArc(tangent1End:tangent2End:)` does given the corner
    // and the next point.
    path.move(to: midpoint(points[points.count - 1], points[0]))
    for index in points.indices {
        let corner = points[index]
        let next = points[(index + 1) % points.count]
        path.addArc(tangent1End: corner, tangent2End: next, radius: radius)
    }
    path.closeSubpath()

    return path.copy(using: [transform]) ?? path
}

func midpoint(_ a: CGPoint, _ b: CGPoint) -> CGPoint {
    CGPoint(x: (a.x + b.x) / 2, y: (a.y + b.y) / 2)
}

/// The mark, ready to fill.
func kestrelPath(in side: Double) -> CGPath {
    polygon(markOutline, radius: 18, transform: glyphTransform(side: side))
}

// MARK: - Drawing

/// Renders the icon at one pixel size.
func render(side: Int) -> Data? {
    let size = Double(side)
    guard let context = CGContext(
        data: nil,
        width: side,
        height: side,
        bitsPerComponent: 8,
        bytesPerRow: 0,
        space: CGColorSpace(name: CGColorSpace.sRGB)!,
        bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
    ) else { return nil }

    context.setAllowsAntialiasing(true)
    context.interpolationQuality = .high

    let scale = size / canvas
    let plate = CGRect(
        x: plateInset * scale,
        y: plateInset * scale,
        width: size - 2 * plateInset * scale,
        height: size - 2 * plateInset * scale
    )
    let platePath = CGPath(
        roundedRect: plate,
        cornerWidth: cornerRadius * scale,
        cornerHeight: cornerRadius * scale,
        transform: nil
    )

    func color(_ red: Double, _ green: Double, _ blue: Double, _ alpha: Double = 1) -> CGColor {
        CGColor(srgbRed: red / 255, green: green / 255, blue: blue / 255, alpha: alpha)
    }

    // A shadow under the plate, so the icon has the same lift as the ones
    // beside it in the Dock. Skipped below 64px, where it only muddies things.
    if side >= 64 {
        context.saveGState()
        context.setShadow(
            offset: CGSize(width: 0, height: -size * 0.012),
            blur: size * 0.03,
            color: color(0, 0, 0, 0.45)
        )
        context.addPath(platePath)
        context.setFillColor(color(16, 24, 40))
        context.fillPath()
        context.restoreGState()
    }

    // The plate: a deep slate blue, lighter at the top.
    context.saveGState()
    context.addPath(platePath)
    context.clip()
    let plateGradient = CGGradient(
        colorsSpace: CGColorSpace(name: CGColorSpace.sRGB)!,
        colors: [color(44, 62, 102), color(20, 30, 54), color(13, 19, 36)] as CFArray,
        locations: [0, 0.55, 1]
    )!
    context.drawLinearGradient(
        plateGradient,
        start: CGPoint(x: 0, y: plate.maxY),
        end: CGPoint(x: 0, y: plate.minY),
        options: []
    )

    // A soft sheen across the top third, which is what stops the plate reading
    // as flat colour at large sizes.
    if side >= 64 {
        let sheen = CGGradient(
            colorsSpace: CGColorSpace(name: CGColorSpace.sRGB)!,
            colors: [color(255, 255, 255, 0.16), color(255, 255, 255, 0)] as CFArray,
            locations: [0, 1]
        )!
        context.drawLinearGradient(
            sheen,
            start: CGPoint(x: 0, y: plate.maxY),
            end: CGPoint(x: 0, y: plate.midY + plate.height * 0.08),
            options: []
        )
    }
    context.restoreGState()

    // A hairline along the top edge, the usual trick for making a dark plate
    // look like an object rather than a hole.
    if side >= 128 {
        context.saveGState()
        context.addPath(platePath)
        context.setLineWidth(max(1, size * 0.004))
        context.setStrokeColor(color(255, 255, 255, 0.18))
        context.strokePath()
        context.restoreGState()
    }

    // The mark, in kestrel rufous warmed towards amber so it holds its own
    // against the blue at small sizes.
    context.saveGState()
    context.addPath(kestrelPath(in: size))
    context.clip()
    let birdGradient = CGGradient(
        colorsSpace: CGColorSpace(name: CGColorSpace.sRGB)!,
        colors: [color(247, 190, 104), color(230, 143, 54), color(198, 96, 38)] as CFArray,
        locations: [0, 0.5, 1]
    )!
    context.drawLinearGradient(
        birdGradient,
        start: CGPoint(x: 0, y: size * 0.86),
        end: CGPoint(x: 0, y: size * 0.12),
        options: []
    )
    context.restoreGState()

    guard let image = context.makeImage() else { return nil }
    let bitmap = NSBitmapImageRep(cgImage: image)
    bitmap.size = NSSize(width: size, height: size)
    return bitmap.representation(using: .png, properties: [:])
}

// MARK: - Writing the iconset

let root = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
let iconset = root.appendingPathComponent("build/AppIcon.iconset")
try? FileManager.default.removeItem(at: iconset)
try FileManager.default.createDirectory(at: iconset, withIntermediateDirectories: true)

/// The sizes `iconutil` expects, each at 1x and 2x.
let sizes = [16, 32, 128, 256, 512]
for size in sizes {
    for scale in [1, 2] {
        let pixels = size * scale
        guard let png = render(side: pixels) else {
            FileHandle.standardError.write(Data("could not render \(pixels)px\n".utf8))
            exit(1)
        }
        let name = scale == 1 ? "icon_\(size)x\(size).png" : "icon_\(size)x\(size)@2x.png"
        try png.write(to: iconset.appendingPathComponent(name))
    }
}

// A standalone PNG too, for the README and for looking at the thing.
if let png = render(side: 1024) {
    try png.write(to: root.appendingPathComponent("build/AppIcon-1024.png"))
}

let iconutil = Process()
iconutil.executableURL = URL(fileURLWithPath: "/usr/bin/iconutil")
iconutil.arguments = [
    "--convert", "icns",
    "--output", root.appendingPathComponent("Resources/AppIcon.icns").path,
    iconset.path
]
try iconutil.run()
iconutil.waitUntilExit()
guard iconutil.terminationStatus == 0 else {
    FileHandle.standardError.write(Data("iconutil failed\n".utf8))
    exit(1)
}

print("wrote Resources/AppIcon.icns and build/AppIcon-1024.png")
