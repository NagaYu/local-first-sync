import Foundation
import Testing
@testable import LocalFirstSync

/// Per-(content, peer) tracking: the same photo carries independent status
/// against each peer, and one peer's interruption never touches another's
/// recorded state.
@Suite("Sync state")
struct SyncStateTests {
    // MARK: Independent per-peer status

    @Test("the same content holds different statuses against different peers")
    func independentStatusPerPeer() async throws {
        let store = try await SyncStateStore.inMemory()
        let photo = "asset-1"

        try await store.markSynced(content: photo, peer: "mac")
        try await store.markPending(content: photo, peer: "ipad")

        let mac = try await store.record(content: photo, peer: "mac")
        let ipad = try await store.record(content: photo, peer: "ipad")
        #expect(mac?.status == .synced)
        #expect(ipad?.status == .pending)

        let all = try await store.records(forContent: photo)
        #expect(all.count == 2)
    }

    @Test("three peers track the same photo through different lifecycles at once")
    func threePeerLifecycles() async throws {
        let store = try await SyncStateStore.inMemory()
        let photo = "asset-42"

        // iPhone→Mac synced; iPhone→iPad declined; iPhone→Vision Pro failing.
        try await store.markPending(content: photo, peer: "mac")
        try await store.markInFlight(content: photo, peer: "mac")
        try await store.markSynced(content: photo, peer: "mac")

        try await store.markPending(content: photo, peer: "ipad")
        try await store.markDeclined(content: photo, peer: "ipad")

        try await store.markPending(content: photo, peer: "vision")
        try await store.markInFlight(content: photo, peer: "vision")
        try await store.markFailed(content: photo, peer: "vision", reason: "link dropped")

        let records = try await store.records(forContent: photo)
        let byPeer = Dictionary(uniqueKeysWithValues: records.map { ($0.peerID, $0) })
        #expect(byPeer["mac"]?.status == .synced)
        #expect(byPeer["ipad"]?.status == .declined)
        #expect(byPeer["vision"]?.status == .failed)
        #expect(byPeer["vision"]?.lastError == "link dropped")
        #expect(byPeer["mac"]?.attemptCount == 1)
        #expect(byPeer["vision"]?.attemptCount == 1)
    }

    @Test("interrupting one peer's transfer never rewrites the others' records")
    func interruptionIsolation() async throws {
        let store = try await SyncStateStore.inMemory()
        let photo = "asset-7"

        try await store.markSynced(content: photo, peer: "mac")
        let macBefore = try await store.record(content: photo, peer: "mac")

        // The iPad transfer is interrupted mid-flight.
        try await store.markInFlight(content: photo, peer: "ipad")
        try await store.markFailed(content: photo, peer: "ipad", reason: "interrupted")

        let macAfter = try await store.record(content: photo, peer: "mac")
        #expect(macAfter?.status == .synced)
        #expect(macAfter?.updatedAt == macBefore?.updatedAt)

        let ipad = try await store.record(content: photo, peer: "ipad")
        #expect(ipad?.status == .failed)
    }

    @Test("an interrupted peer recovers by retrying, and the attempt count reflects it")
    func interruptionRecovery() async throws {
        let store = try await SyncStateStore.inMemory()
        let photo = "asset-9"

        try await store.markPending(content: photo, peer: "ipad")
        try await store.markInFlight(content: photo, peer: "ipad")
        try await store.markFailed(content: photo, peer: "ipad", reason: "interrupted")

        // Retry after the interruption.
        try await store.markPending(content: photo, peer: "ipad")
        try await store.markInFlight(content: photo, peer: "ipad")
        try await store.markSynced(content: photo, peer: "ipad")

        let record = try await store.record(content: photo, peer: "ipad")
        #expect(record?.status == .synced)
        #expect(record?.attemptCount == 2)
        #expect(record?.lastError == nil)
    }

    // MARK: Upsert semantics

    @Test("repeated transitions upsert one row per (content, peer), never duplicates")
    func upsertNoDuplicates() async throws {
        let store = try await SyncStateStore.inMemory()
        for _ in 0..<5 {
            try await store.markPending(content: "asset-1", peer: "mac")
            try await store.markSynced(content: "asset-1", peer: "mac")
        }
        let records = try await store.records(forContent: "asset-1")
        #expect(records.count == 1)
        #expect(records.first?.status == .synced)
    }

    @Test("reset removes exactly one pair")
    func resetOnePair() async throws {
        let store = try await SyncStateStore.inMemory()
        try await store.markSynced(content: "a", peer: "mac")
        try await store.markSynced(content: "a", peer: "ipad")

        try await store.reset(content: "a", peer: "mac")

        #expect(try await store.record(content: "a", peer: "mac") == nil)
        #expect(try await store.record(content: "a", peer: "ipad")?.status == .synced)
    }

    // MARK: Queries

    @Test("contentIDs(notSyncedWith:) answers the what-still-needs-sending question per peer")
    func notSyncedQuery() async throws {
        let store = try await SyncStateStore.inMemory()
        let library = ["p1", "p2", "p3"]

        try await store.markSynced(content: "p1", peer: "mac")
        try await store.markFailed(content: "p2", peer: "mac", reason: "x")
        // p3 never offered to the mac at all.
        try await store.markSynced(content: "p3", peer: "ipad")

        let needsMac = try await store.contentIDs(notSyncedWith: "mac", from: library)
        #expect(needsMac == ["p2", "p3"])
        let needsIpad = try await store.contentIDs(notSyncedWith: "ipad", from: library)
        #expect(needsIpad == ["p1", "p2"])
    }

    @Test("records persist across store reopen on disk")
    func persistenceAcrossReopen() async throws {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("lfs-store-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        let storeURL = url.appendingPathComponent("test.store")

        do {
            let store = try await SyncStateStore.onDisk(at: storeURL)
            try await store.markSynced(content: "asset-1", peer: "mac")
            try await store.markFailed(content: "asset-1", peer: "ipad", reason: "interrupted")
        }

        let reopened = try await SyncStateStore.onDisk(at: storeURL)
        let records = try await reopened.records(forContent: "asset-1")
        let byPeer = Dictionary(uniqueKeysWithValues: records.map { ($0.peerID, $0.status) })
        #expect(byPeer == ["mac": .synced, "ipad": .failed])

        try? FileManager.default.removeItem(at: url)
    }
}

/// The engine drives the store end to end: transfers recorded per peer as
/// they progress, with real three-peer broadcasts.
@Suite("Sync state integration")
struct SyncStateIntegrationTests {
    @Test("a three-peer broadcast records synced, declined, and failed independently")
    func broadcastRecordsPerPeer() async throws {
        let harness = Harness.make()
        let store = try await SyncStateStore.inMemory()
        await harness.sync.attachSyncState(store)

        let boxes = try await connectPeers(harness, ["iPhone", "iPad", "Mac"])
        let (iPhone, iPad, mac) = (boxes[0], boxes[1], boxes[2])

        // Give each peer a stable identity, as the hello exchange would.
        for (box, id) in [(iPhone, "id-iphone"), (iPad, "id-ipad"), (mac, "id-mac")] {
            harness.transport.continuation.yield(.dataReceived(
                from: box,
                encode(.hello(HelloPayload(stableID: id, displayName: box.peerID.displayName, protocolVersion: 1)))
            ))
        }
        await eventually {
            await harness.sync.connectedPeers.allSatisfy { $0.stableID != nil }
        }

        let file = try makeTempFile(named: "photo.heic", byteCount: 100)
        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        let contentID = file.absoluteString

        await eventually { harness.transport.offers(to: mac).count == 1 }

        // Mac completes; iPad declines; iPhone accepts but the link drops.
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.accept(transferID: transfer.id))))
        harness.transport.continuation.yield(.dataReceived(from: iPad, encode(.decline(transferID: transfer.id))))
        harness.transport.continuation.yield(.dataReceived(from: iPhone, encode(.accept(transferID: transfer.id))))
        await eventually { harness.transport.sentResources.count == 2 }

        let macToken = try #require(harness.transport.sentResources.first { $0.peer == mac }?.token)
        let iPhoneToken = try #require(harness.transport.sentResources.first { $0.peer == iPhone }?.token)

        harness.transport.continuation.yield(.resourceSendFinished(token: macToken, error: nil))
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.received(transferID: transfer.id))))
        harness.transport.continuation.yield(.resourceSendFinished(token: iPhoneToken, error: "connection reset"))

        _ = try await transfer.waitUntilCompleted()

        // Store writes are chained; wait for the final states to land.
        let landed = await eventually {
            let mac = try? await store.record(content: contentID, peer: "id-mac")
            let ipad = try? await store.record(content: contentID, peer: "id-ipad")
            let iphone = try? await store.record(content: contentID, peer: "id-iphone")
            return mac?.status == .synced && ipad?.status == .declined && iphone?.status == .failed
        }
        #expect(landed)

        // Full lifecycle recorded for the synced peer.
        let macRecord = try await store.record(content: contentID, peer: "id-mac")
        #expect(macRecord?.attemptCount == 1)
    }

    @Test("synced is recorded only after the peer's confirmation, not at delivery")
    func syncedOnlyAfterConfirmation() async throws {
        var configuration = LocalSyncConfiguration()
        configuration.confirmationTimeout = 2
        let harness = Harness.make(configuration: configuration)
        let store = try await SyncStateStore.inMemory()
        await harness.sync.attachSyncState(store)

        let boxes = try await connectPeers(harness, ["Mac"])
        let mac = boxes[0]
        harness.transport.continuation.yield(.dataReceived(
            from: mac,
            encode(.hello(HelloPayload(stableID: "id-mac", displayName: "Mac", protocolVersion: 1)))
        ))

        let file = try makeTempFile(named: "a.bin", byteCount: 10)
        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        let contentID = file.absoluteString

        await eventually { harness.transport.offers(to: mac).count == 1 }
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.accept(transferID: transfer.id))))
        await eventually { harness.transport.sentResources.count == 1 }
        let token = harness.transport.sentResources[0].token

        // Delivery completed — but no confirmation yet: still inFlight.
        harness.transport.continuation.yield(.resourceSendFinished(token: token, error: nil))
        await eventually {
            (try? await store.record(content: contentID, peer: "id-mac"))?.status == .inFlight
        }
        let midway = try await store.record(content: contentID, peer: "id-mac")
        #expect(midway?.status == .inFlight)

        // Only the peer's own confirmation flips it to synced.
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.received(transferID: transfer.id))))
        _ = try await transfer.waitUntilCompleted()
        let confirmed = await eventually {
            (try? await store.record(content: contentID, peer: "id-mac"))?.status == .synced
        }
        #expect(confirmed)
    }

    @Test("a missing confirmation records failed, not synced")
    func missingConfirmationIsFailure() async throws {
        var configuration = LocalSyncConfiguration()
        configuration.confirmationTimeout = 0.1
        let harness = Harness.make(configuration: configuration)
        let store = try await SyncStateStore.inMemory()
        await harness.sync.attachSyncState(store)

        let boxes = try await connectPeers(harness, ["Mac"])
        let mac = boxes[0]
        harness.transport.continuation.yield(.dataReceived(
            from: mac,
            encode(.hello(HelloPayload(stableID: "id-mac", displayName: "Mac", protocolVersion: 1)))
        ))

        let file = try makeTempFile(named: "a.bin", byteCount: 10)
        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        await eventually { harness.transport.offers(to: mac).count == 1 }
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.accept(transferID: transfer.id))))
        await eventually { harness.transport.sentResources.count == 1 }
        harness.transport.continuation.yield(.resourceSendFinished(token: harness.transport.sentResources[0].token, error: nil))

        // No `received` ever arrives; the confirmation watchdog fires.
        await #expect(throws: LocalSyncError.self) {
            try await transfer.waitUntilCompleted()
        }
        let failed = await eventually {
            (try? await store.record(content: file.absoluteString, peer: "id-mac"))?.status == .failed
        }
        #expect(failed)
    }
}
