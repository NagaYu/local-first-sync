# PhotoSyncApp

A small SwiftUI app demonstrating [local-first-sync](../..) with **all of a
user's devices in one session**: run it on an iPhone and a Mac (the common
two-device case), then add an iPad and watch it become one more row in the
same UI — broadcasting shows an independent progress bar per device, and the
sync ledger tracks "synced with the Mac" and "synced with the iPad" as
separate facts about the same photo.

What it exercises:

- **Devices tab** — discovery (`nearbyPeers()`), connecting
  (`connect(to:)`), and incoming connection requests that must be explicitly
  accepted (`connectionRequests()`).
- **Send tab** — pick photos, broadcast with `send(photo:to:
  .allConnectedPeers)`, watch per-peer progress (never a blended average),
  including the "preparing" phase when an original needs downloading from the
  user's own iCloud Photos first.
- **Activity tab** — AirDrop-style accept/decline for incoming offers
  (`incomingTransfers()`), receive progress, saving into the photo library
  with Live Photo pairing preserved (`PhotoLibraryImporter`), and the
  per-(photo, device) `SyncStateStore` ledger.

## Building

The app needs an Xcode project (SwiftPM alone can't build iOS app targets).
Two options:

### Option A: XcodeGen

```sh
brew install xcodegen   # if needed
cd Examples/PhotoSyncApp
xcodegen generate
open PhotoSyncApp.xcodeproj
```

### Option B: manual Xcode setup

1. In Xcode: File → New → Project → **Multiplatform App**, name it
   `PhotoSyncApp`, save it anywhere.
2. Delete the template's generated Swift files and drag the files from
   `PhotoSyncApp/` (this directory) into the target.
3. File → Add Package Dependencies → **Add Local…** → select the repository
   root (two levels up); add the `LocalFirstSync` product to the target.
4. Add the Info.plist keys and entitlements listed below.
5. Set your development team, then build & run on each device.

## Required Info.plist keys (both platforms)

| Key | Value |
|---|---|
| `NSLocalNetworkUsageDescription` | Why the app talks to nearby devices |
| `NSBonjourServices` | `_lfs-photosync._tcp` **and** `_lfs-photosync._udp` |
| `NSPhotoLibraryUsageDescription` | Why the app reads photos |
| `NSPhotoLibraryAddUsageDescription` | Why the app saves photos |

Without `NSBonjourServices` + `NSLocalNetworkUsageDescription`, iOS 14+
silently discovers nothing. The Bonjour entries must match the
`LocalSync(serviceName:)` value — here `lfs-photosync` — in both `_tcp` and
`_udp` forms.

## Required entitlements (macOS, sandboxed)

- `com.apple.security.network.client` and
  `com.apple.security.network.server` — Multipeer Connectivity needs both.
- `com.apple.security.personal-information.photos-library` — photo library
  access. On macOS, PhotoKit only sees the **System Photo Library**.

## Trying the three-device scenario

1. Run the app on two devices (say iPhone + Mac). Each advertises and browses
   automatically.
2. On the iPhone, tap the Mac in **Nearby** → the Mac shows a connection
   request → **Accept**.
3. Send a photo from the iPhone: the Mac gets an offer, accepts, the photo
   arrives, **Save to Photos**. Check the sync ledger on the iPhone.
4. Now run the app on an iPad and connect it the same way — the session holds
   all three at once.
5. Send a photo to all: two progress bars advance independently; decline on
   one device and watch the other complete unaffected, with the ledger
   recording `Declined` and `Synced` separately.
