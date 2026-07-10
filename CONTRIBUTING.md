# Contributing to local-first-sync

Thanks for helping! This project is a standard Swift package:

```sh
swift build
swift test
```

Requires Xcode 16+ (Swift 6.0). CI runs both on the newest toolchain
(macos-26) and the oldest supported one (macos-15 / Xcode 16.4) — please keep
the code buildable with Swift 6.0 language features.

## What unit tests can and cannot cover

All 47 unit tests run against **scripted fakes** (`FakeTransport`,
`FakeExporter`): they drive the engine with exact delegate-event
interleavings — including the three-peers-at-once orderings that expose
bridging bugs — and they assert the consent handshake, per-peer progress
independence, and per-(content, peer) sync-state isolation.

Two things deliberately never run in CI:

- **Real radios.** A hosted runner is a single VM with no second peer, and
  macOS 15+ local-network privacy blocks headless browsing/advertising
  anyway. Anything touching `MCNearbyServiceBrowser`/`Advertiser` for real
  needs physical devices.
- **PhotoKit.** A headless test process has no photo-library usage
  description and gets killed by TCC. The export/import paths are behind
  protocols and covered by fakes; the real paths need a device with photos.

So: **real multi-device transfer testing is manual.** Please run the
checklist below on physical hardware before tagging a release or approving
changes to `Transport.swift`, `Transfer.swift`, or the engine.

## Manual test checklist

Use [`Examples/PhotoSyncApp`](Examples/PhotoSyncApp) on real devices signed
with your team. **Two physical devices (e.g. an iPhone and a Mac) are enough
to verify the primary scenario end to end** — the third device extends the
same checklist rather than requiring a separate one.

### With two devices (the primary scenario)

- [ ] **Discovery** — each device appears in the other's *Nearby* list with
      the right name, on Wi-Fi and with Wi-Fi off (Bluetooth fallback).
- [ ] **Connecting** — tapping *Connect* shows a connection request on the
      other device; nothing connects until *Accept* is tapped; *Decline*
      fails the attempt on the initiating side.
- [ ] **Sending with progress** — send a large video: the receiver sees the
      offer with filename and size; after accepting, both sides show smooth
      progress; the received file is byte-identical (same size, plays
      correctly) and format-preserved (no re-encode).
- [ ] **Live Photo round-trip** — send a Live Photo, save it on the receiver:
      it must still animate (pairing preserved).
- [ ] **Accept/decline** — decline an offer: the sender shows *Declined*, and
      **no** transfer data flows; sync ledger records `Declined`.
- [ ] **iCloud original** — with "Optimize Storage" on and the original
      evicted, sending shows the *preparing* phase first, then transfers the
      full-quality original.
- [ ] **Interrupted transfer recovery** — start a large transfer, then turn
      off Wi-Fi + Bluetooth on the receiver mid-flight: the sender marks that
      transfer `Failed` (ledger: `failed`, not `synced`); reconnect and
      resend: it completes and the ledger flips to `Synced` with the attempt
      count incremented.

### Adding a third device (the case that must not break the first two)

- [ ] **Three in one session** — connect the third device: all three show 2
      connected peers each; no existing connection drops.
- [ ] **Broadcast with independent progress** — send a photo to *all*: each
      receiving device gets its own offer; the sender shows **one progress
      bar per device advancing at its own rate** (throttle one device's link
      — e.g. move it to the edge of range — and confirm the bars diverge
      rather than showing one blended value).
- [ ] **One declines, others unaffected** — decline on one device: the other
      still receives and completes; ledger shows `Declined` and `Synced` for
      the same photo, per device.
- [ ] **One interrupted, others unaffected** — kill one receiver's radio
      mid-broadcast: that device's row fails; the other completes; the
      ledger keeps `failed` and `synced` independently.
- [ ] **Two-device regression** — after removing the third device, re-run the
      two-device *Sending with progress* item to confirm nothing regressed.

## Pull requests

- Keep the public API surface free of `try!`, `as!`, and force-unwraps.
- `MCSession` encryption stays `.required` — this is a hard project
  constraint, not a preference.
- New engine behavior needs fake-driven unit tests covering **both** one
  connected peer and three-or-more, and interleaving orders, not just the
  happy path.
- Note in your PR description which manual checklist items you ran, on which
  hardware.
