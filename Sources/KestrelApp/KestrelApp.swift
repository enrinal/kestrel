import SwiftUI

@main
struct KestrelApp: App {
    @State private var store = ClusterStore()

    var body: some Scene {
        WindowGroup("Kestrel", id: WindowID.main) {
            RootView()
                .environment(store)
                .frame(minWidth: 900, minHeight: 560)
        }
        .commands { KestrelCommands(store: store) }

        Window("About Kestrel", id: WindowID.about) {
            AboutView()
        }
        .windowResizability(.contentSize)
        .defaultPosition(.center)
    }
}

enum WindowID {
    static let main = "kestrel.main"
    static let about = "kestrel.about"
}
