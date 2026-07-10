# local-first-sync

[![CI](https://github.com/NagaYu/local-first-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/NagaYu/local-first-sync/actions/workflows/ci.yml)
[![Swift versions](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2FNagaYu%2Flocal-first-sync%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/NagaYu/local-first-sync)
[![Platforms](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2FNagaYu%2Flocal-first-sync%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/NagaYu/local-first-sync)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Photo and file sync between a user's own nearby Apple devices.
**No iCloud. No server. No account. And it works for all your devices at
once — not just two.**

A user with an iPhone, an iPad, and a Mac has *one* set of photos and *three*
devices. `local-first-sync` connects them directly over Wi-Fi/Bluetooth
(Multipeer Connectivity — the same technology family as AirDrop), with:

- a modern **async/await + AsyncSequence** API — no delegates, no completion
  handlers
- **genuinely multi-peer sessions**: one session holds every connected device;
  two devices is simply the smallest case of the same code path, not a
  special one
- **first-class photo transfer**: `PHAsset` originals — photos, Live Photos,
  video — format-preserved, never re-encoded, streamed to disk
- **real per-peer progress**: broadcasting to three devices gives you three
  independent progress streams, never one blended average — if the iPad
  finishes while the Mac is at 40%, your UI can show exactly that
- **SwiftData-backed sync state per (content, peer) pair**: "synced with the
  Mac" and "synced with the iPad" are independent facts about the same photo,
  and they're stored that way
- **AirDrop-style consent**: nothing is ever received automatically — every
  incoming connection *and* every incoming transfer requires an explicit
  accept on the receiving device

## Quickstart

```swift
import LocalFirstSync

let sync = LocalSync(serviceName: "my-app-sync")

// Discover and connect — a session holds multiple simultaneously
// connected peers. Call connect(to:) once per additional device:
// iPhone, iPad, and Mac can all be in the same session at once.
for await peer in sync.nearbyPeers() {
    // present peer.displayName; let the user choose
}
try await sync.connect(to: peer)

// Send a photo to every connected peer at once.
for await update in sync.send(photo: asset, to: .allConnectedPeers) {
    // update.peer, update.progress.fractionCompleted —
    // one progress series per peer, never a blended average
}

// Or target specific peers, and just await the outcome.
let summary = try await sync.send(photo: asset, to: [ipad, mac]).waitUntilCompleted()
```

Receiving — both steps are explicit, nothing happens silently:

```swift
// 1. Someone wants to join your session.
for await request in sync.connectionRequests() {
    try await request.accept()          // or await request.decline()
}

// 2. A connected peer offers you content. No bytes flow until you accept.
for await incoming in sync.incomingTransfers() {
    // show incoming.from.displayName, incoming.items, incoming.totalByteCount
    let session = try await incoming.accept()
    let files = try await session.files()
    try await PhotoLibraryImporter.saveToPhotoLibrary(files)  // Live Photo pairing preserved
}
```

Per-(photo, device) sync state:

```swift
let store = try await SyncStateStore.onDisk()
await sync.attachSyncState(store)

// Later: what still needs sending to the iPad specifically?
let pending = try await store.contentIDs(notSyncedWith: ipad.syncIdentity,
                                         from: libraryAssetIDs)
```

A record only becomes `.synced` when **that specific peer** confirms it
received and stored the content. An interrupted transfer to one device marks
only that device's record `.failed` — every other device's state is untouched.

## The trust model, and why

Discovering a nearby device is never enough to receive data from it:

1. **Connections need consent.** An invitation surfaces as a
   `ConnectionRequest`; until your code (or your user) calls `accept()`,
   nothing joins the session. Unanswered requests time out as if declined.
2. **Every transfer needs consent.** Senders first transmit a small offer
   (filenames, sizes, kind). Only after the receiving device explicitly
   accepts does the sender start the actual resource transfer. Anything
   arriving without an accepted offer is deleted, never surfaced.
3. **The session requires encryption** (`MCEncryptionPreference.required`) —
   never the `.none` shortcut common in tutorial code.

This mirrors the UX pattern users already understand from AirDrop: your
devices see each other, and *you* decide what moves between them.

## "My originals are in iCloud" — the distinction that matters

If the user's own Photos library uses *iCloud Photos with Optimize Storage*,
a photo's full-quality original may need to be **downloaded by Photos first**
before it can be sent. That is a property of the user's own library setup,
not of this package: `local-first-sync` handles it transparently (reported
via `transfer.preparationProgress()`), and the transfer itself **always
travels over the local peer-to-peer link — never through a server**. If the
download is impossible (offline), the transfer fails with
`iCloudOriginalUnavailable` rather than silently sending a degraded copy.

## Installation

```swift
dependencies: [
    .package(url: "https://github.com/NagaYu/local-first-sync", from: "0.1.0")
]
```

Requires iOS 17 / macOS 14 / visionOS 1 (the SwiftData floor) and Swift 6.0+.

### Required Info.plist keys (iOS 14+ local-network privacy)

Your app must declare, or discovery silently finds nothing:

```xml
<key>NSLocalNetworkUsageDescription</key>
<string>Connects directly to your own nearby devices. Nothing leaves your local network.</string>
<key>NSBonjourServices</key>
<array>
    <string>_my-app-sync._tcp</string>
    <string>_my-app-sync._udp</string>
</array>
```

(Replace `my-app-sync` with your `LocalSync(serviceName:)` value — both
`_tcp` and `_udp` entries are needed.) Sandboxed Mac apps additionally need
the `network.client` + `network.server` entitlements. Photo access needs the
usual `NSPhotoLibraryUsageDescription` / `NSPhotoLibraryAddUsageDescription`.

## Example app

[`Examples/PhotoSyncApp`](Examples/PhotoSyncApp) is a SwiftUI app that runs
on iPhone, iPad, and Mac simultaneously: discovery, connection approval,
multi-photo broadcast with per-device progress bars, incoming accept/decline,
save-to-library, and the per-(photo, device) sync ledger. Two devices show
the primary flow; a third becomes just one more row.

## Design notes

- **One event mailbox, one consumer.** Multipeer Connectivity's delegate
  callbacks arrive on a private queue; each is converted synchronously into a
  value event and consumed by a single actor loop, preserving order and
  making three-peer interleavings safe by construction. Continuations are
  keyed per peer and resolved take-then-resume, so completions, timeouts, and
  cancellation can race without double-resume.
- **`sendResource` per peer.** Broadcasts fan out one
  `sendResource(at:withName:toPeer:)` call per target — the framework API
  that handles large files properly and reports real per-peer `Progress`.
- **Transport is an internal seam.** Apple deprecated Multipeer Connectivity
  in the OS 27 betas (it remains fully functional on all shipping OS
  versions, and is still the only high-level nearby-devices API available at
  this package's deployment floor). The MPC specifics live behind an internal
  transport protocol so a Network-framework transport can be added without
  touching the public API.

## Prior art & credits

- **[Multipeer Connectivity](https://developer.apple.com/documentation/multipeerconnectivity)**
  is the foundation and deserves the credit for the hard parts: discovery,
  encrypted sessions, and multi-peer transport over Wi-Fi and Bluetooth. It
  works identically across iPhone, iPad, and Mac — this package adds no
  per-device-type code, just a design that takes multi-peer sessions
  seriously.
- **[dingwilson/MultiPeer](https://github.com/dingwilson/MultiPeer)** (2019)
  demonstrated the "wrap MPC for easy data transmission" idea years ago, with
  a delegate-based API around typed `Data` messages. Honest credit to that
  lineage — and an honest statement of what's different here: Swift
  Concurrency throughout, first-class large-resource/photo transfer with
  per-peer progress, and per-(content, peer) sync state designed for
  three-or-more-device households from the start.
- **[insidegui/MultipeerKit](https://github.com/insidegui/MultipeerKit)**
  (2021) brought an elegant `Codable`-message API to MPC. Same distinction:
  message exchange vs. this package's resource-transfer-with-consent focus.
- **AirDrop** is the built-in, user-facing feature in the same technology
  family. This package is for developers who want that no-cloud,
  nearby-device experience inside their own app's UI and flow — across all of
  a user's devices at once.

## Testing

`swift test` runs 47 unit tests against scripted fakes — including
three-peers-at-once interleavings, consent enforcement, per-peer progress
independence, and per-(content, peer) state isolation. Real multi-device
transfer can't run in CI; [CONTRIBUTING.md](CONTRIBUTING.md) has the manual
two-then-three-device checklist.

## License

[MIT](LICENSE)
