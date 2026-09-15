import SwiftUI

struct AboutView: View {
    private var version: String {
        let bundle = Bundle.main
        let short = bundle.infoDictionary?["CFBundleShortVersionString"] as? String ?? "0.0.0"
        let build = bundle.infoDictionary?["CFBundleVersion"] as? String ?? "0"
        return "Version \(short) (\(build))"
    }

    var body: some View {
        VStack(spacing: 12) {
            Image(systemName: "bird.fill")
                .font(.system(size: 56))
                .foregroundStyle(.tint)
            Text("Kestrel")
                .font(.largeTitle.weight(.semibold))
            Text("A native macOS explorer for Apache Kafka.")
                .foregroundStyle(.secondary)
            Text(version)
                .font(.caption)
                .foregroundStyle(.tertiary)
        }
        .multilineTextAlignment(.center)
        .padding(32)
        .frame(width: 360)
    }
}
