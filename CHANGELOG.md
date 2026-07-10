# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-10

### Added

- `LocalSync`: async/await API over Multipeer Connectivity with one encrypted
  (`.required`) multi-peer `MCSession` — discovery (`nearbyPeers()`),
  explicit connection approval (`connectionRequests()`), and
  `connect(to:)`-per-device up to the framework's 8-peer session limit.
- Photo/file transfer with AirDrop-style consent: offers precede bytes,
  every transfer needs an explicit `accept()` on the receiving device, and
  unsolicited resources are discarded.
- `PHAsset` export via `PHAssetResource` (photos, Live Photos, video),
  format-preserved with no re-encoding, including iCloud-optimized originals
  (reported through `preparationProgress()`).
- Per-peer progress: broadcasts fan out one `sendResource` per target and
  yield independent `TransferUpdate` streams per peer — never a blended
  average.
- `PhotoLibraryImporter` for saving received photos with Live Photo pairing
  preserved.
- `SyncStateStore`: SwiftData-backed per-(content, peer) sync records
  (`pending` / `inFlight` / `synced` / `failed` / `declined`), where `synced`
  requires that specific peer's storage confirmation and one peer's
  interruption never affects another's record.
- `Examples/PhotoSyncApp`: SwiftUI demo for iPhone + iPad + Mac in one
  session.
- 47 unit tests over scripted fakes, covering the one-peer baseline, 3-peer
  interleavings, consent enforcement, and state isolation.

[0.1.0]: https://github.com/NagaYu/local-first-sync/releases/tag/0.1.0
