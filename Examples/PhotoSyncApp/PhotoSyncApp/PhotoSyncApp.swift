import SwiftUI

/// Demonstrates local-first-sync with an iPhone, an iPad, and a Mac all in
/// one session: discovery, explicit connection approval, broadcasting a photo
/// to every connected device with independent per-peer progress, AirDrop-style
/// accept/decline on the receiving side, and per-(photo, device) sync state.
///
/// Two devices are enough to try everything; a third simply appears as one
/// more row everywhere.
@main
struct PhotoSyncApp: App {
    @State private var model = SyncModel()

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environment(model)
                .task { await model.start() }
        }
    }
}
