import Foundation
import Testing
@testable import LocalFirstSync

/// Outgoing and incoming transfer flows: consent before bytes, per-peer
/// progress that is never blended, and failure isolation between peers.
@Suite("Transfers")
struct TransferTests {
    // MARK: Baseline: one connected peer (two-device household)

    @Test("a file transfer to one peer runs offer → accept → send → confirm → completed")
    func singlePeerHappyPath() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["Mac"])
        let mac = boxes[0]
        let file = try makeTempFile(named: "photo.heic", byteCount: 100)

        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        let collector = Task {
            var updates: [TransferUpdate] = []
            for await update in transfer { updates.append(update) }
            return updates
        }

        // 1. The offer goes out, and *no* resource is sent yet.
        await eventually { !harness.transport.offers(to: mac).isEmpty }
        let offer = try #require(harness.transport.offers(to: mac).first)
        #expect(offer.transferID == transfer.id)
        #expect(offer.items.count == 1)
        #expect(offer.items.first?.filename == "photo.heic")
        #expect(offer.totalByteCount == 100)
        #expect(harness.transport.sentResources.isEmpty)

        // 2. The peer accepts → exactly one sendResource for it.
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.accept(transferID: transfer.id))))
        await eventually { harness.transport.sentResources.count == 1 }
        let resource = try #require(harness.transport.sentResources.first)
        #expect(resource.peer == mac)
        let parsed = try #require(WireFormat.parseResourceName(resource.name))
        #expect(parsed.transferID == transfer.id)
        #expect(parsed.filename == "photo.heic")

        // 3. Progress, completion, receiver confirmation.
        harness.transport.continuation.yield(.resourceSendProgressed(token: resource.token, fraction: 0.5, completedBytes: 50, totalBytes: 100))
        harness.transport.continuation.yield(.resourceSendFinished(token: resource.token, error: nil))
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.received(transferID: transfer.id))))

        let summary = try await transfer.waitUntilCompleted()
        #expect(summary.completed.map(\.displayName) == ["Mac"])
        #expect(summary.declined.isEmpty)
        #expect(summary.failed.isEmpty)

        let updates = await collector.value
        #expect(updates.first?.phase.isWaitingForAcceptance == true)
        #expect(updates.contains { $0.progress.fractionCompleted == 0.5 })
        #expect(updates.last?.phase.isCompleted == true)
        #expect(updates.allSatisfy { $0.peer.displayName == "Mac" })
    }

    @Test("no resource is ever sent to a peer that declines")
    func declineSendsNothing() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["Mac"])
        let file = try makeTempFile(named: "photo.heic", byteCount: 10)

        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        await eventually { !harness.transport.offers(to: boxes[0]).isEmpty }
        harness.transport.continuation.yield(.dataReceived(from: boxes[0], encode(.decline(transferID: transfer.id))))

        await #expect(throws: LocalSyncError.self) {
            try await transfer.waitUntilCompleted()
        }
        // The whole transfer finished without one byte of resource data.
        #expect(harness.transport.sentResources.isEmpty)
    }

    @Test("an unanswered offer times out for that peer")
    func offerTimeout() async throws {
        var configuration = LocalSyncConfiguration()
        configuration.acceptTimeout = 0.1
        let harness = Harness.make(configuration: configuration)
        _ = try await connectPeers(harness, ["Mac"])
        let file = try makeTempFile(named: "a.bin", byteCount: 10)

        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        await #expect(throws: LocalSyncError.self) {
            try await transfer.waitUntilCompleted()
        }
        #expect(harness.transport.sentResources.isEmpty)
    }

    @Test("sending with no connected peers fails with noPeersConnected")
    func noPeers() async throws {
        let harness = Harness.make()
        let file = try makeTempFile(named: "a.bin", byteCount: 10)
        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        await #expect(throws: LocalSyncError.self) {
            try await transfer.waitUntilCompleted()
        }
    }

    @Test("an explicit target that is not connected fails alone; the connected one proceeds")
    func explicitTargetNotConnected() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["Mac"])
        let mac = boxes[0]
        let ghost = makePeer("Ghost")
        let file = try makeTempFile(named: "a.bin", byteCount: 10)

        let transfer = harness.sync.send(contentsOf: file, to: [Peer(box: mac), ghost])
        await eventually { !harness.transport.offers(to: mac).isEmpty }
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.accept(transferID: transfer.id))))
        await eventually { harness.transport.sentResources.count == 1 }
        let token = harness.transport.sentResources[0].token
        harness.transport.continuation.yield(.resourceSendFinished(token: token, error: nil))
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.received(transferID: transfer.id))))

        let summary = try await transfer.waitUntilCompleted()
        #expect(summary.completed.map(\.displayName) == ["Mac"])
        #expect(summary.failed.map(\.peer.displayName) == ["Ghost"])
    }

    // MARK: Three peers — independent progress, never blended

    @Test("broadcasting to three peers yields independent per-peer progress")
    func threePeerIndependentProgress() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["iPhone", "iPad", "Mac"])
        let (iPhone, iPad, mac) = (boxes[0], boxes[1], boxes[2])
        let file = try makeTempFile(named: "photo.heic", byteCount: 100)

        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        let collector = Task {
            var updates: [TransferUpdate] = []
            for await update in transfer { updates.append(update) }
            return updates
        }

        // All three get offers; iPad and Mac accept, iPhone declines.
        await eventually {
            !harness.transport.offers(to: iPhone).isEmpty
                && !harness.transport.offers(to: iPad).isEmpty
                && !harness.transport.offers(to: mac).isEmpty
        }
        harness.transport.continuation.yield(.dataReceived(from: iPad, encode(.accept(transferID: transfer.id))))
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.accept(transferID: transfer.id))))
        harness.transport.continuation.yield(.dataReceived(from: iPhone, encode(.decline(transferID: transfer.id))))

        await eventually { harness.transport.sentResources.count == 2 }
        let iPadToken = try #require(harness.transport.sentResources.first { $0.peer == iPad }?.token)
        let macToken = try #require(harness.transport.sentResources.first { $0.peer == mac }?.token)

        // Interleaved, deliberately unequal progress: the iPad finishes while
        // the Mac is still at 40%.
        harness.transport.continuation.yield(.resourceSendProgressed(token: macToken, fraction: 0.4, completedBytes: 40, totalBytes: 100))
        harness.transport.continuation.yield(.resourceSendProgressed(token: iPadToken, fraction: 0.9, completedBytes: 90, totalBytes: 100))
        harness.transport.continuation.yield(.resourceSendFinished(token: iPadToken, error: nil))
        harness.transport.continuation.yield(.dataReceived(from: iPad, encode(.received(transferID: transfer.id))))

        // Only after the iPad completed does the Mac progress further — the
        // ordering assertion below proves the iPad's completion preceded it.
        harness.transport.continuation.yield(.resourceSendProgressed(token: macToken, fraction: 0.8, completedBytes: 80, totalBytes: 100))
        harness.transport.continuation.yield(.resourceSendFinished(token: macToken, error: nil))
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.received(transferID: transfer.id))))

        let summary = try await transfer.waitUntilCompleted()
        #expect(Set(summary.completed.map(\.displayName)) == ["iPad", "Mac"])
        #expect(summary.declined.map(\.displayName) == ["iPhone"])

        let updates = await collector.value

        // Per-peer series are distinct: each update names its peer, and the
        // progress values per peer match what that peer was told — no blend.
        let macProgress = updates.filter { $0.peer.displayName == "Mac" }.map(\.progress.fractionCompleted)
        let iPadProgress = updates.filter { $0.peer.displayName == "iPad" }.map(\.progress.fractionCompleted)
        #expect(macProgress.contains(0.4))
        #expect(macProgress.contains(0.8))
        #expect(iPadProgress.contains(0.9))
        // 0.9 was iPad-only; 0.4/0.8 were Mac-only. A blended average would
        // put intermediate values in both series.
        #expect(!macProgress.contains(0.9))
        #expect(!iPadProgress.contains(0.4))

        // Order across phases per peer: iPad completed before Mac's 0.8 update.
        let iPadCompletedIndex = try #require(updates.firstIndex { $0.peer.displayName == "iPad" && $0.phase.isCompleted })
        let macLateProgressIndex = try #require(updates.firstIndex { $0.peer.displayName == "Mac" && $0.progress.fractionCompleted == 0.8 })
        #expect(iPadCompletedIndex < macLateProgressIndex)
    }

    @Test("one peer's failed transfer leaves the other peers' transfers untouched")
    func failureIsolation() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["iPad", "Mac"])
        let (iPad, mac) = (boxes[0], boxes[1])
        let file = try makeTempFile(named: "a.bin", byteCount: 100)

        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        await eventually { !harness.transport.offers(to: iPad).isEmpty && !harness.transport.offers(to: mac).isEmpty }
        harness.transport.continuation.yield(.dataReceived(from: iPad, encode(.accept(transferID: transfer.id))))
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.accept(transferID: transfer.id))))
        await eventually { harness.transport.sentResources.count == 2 }

        let iPadToken = try #require(harness.transport.sentResources.first { $0.peer == iPad }?.token)
        let macToken = try #require(harness.transport.sentResources.first { $0.peer == mac }?.token)

        // The iPad's link errors mid-transfer; the Mac finishes normally.
        harness.transport.continuation.yield(.resourceSendFinished(token: iPadToken, error: "connection lost"))
        harness.transport.continuation.yield(.resourceSendFinished(token: macToken, error: nil))
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.received(transferID: transfer.id))))

        let summary = try await transfer.waitUntilCompleted()
        #expect(summary.completed.map(\.displayName) == ["Mac"])
        #expect(summary.failed.map(\.peer.displayName) == ["iPad"])
    }

    @Test("a peer disconnecting mid-transfer fails that peer only and cancels its send")
    func disconnectMidTransfer() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["iPad", "Mac"])
        let (iPad, mac) = (boxes[0], boxes[1])
        let file = try makeTempFile(named: "a.bin", byteCount: 100)

        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        await eventually { harness.transport.offers(to: iPad).count == 1 && harness.transport.offers(to: mac).count == 1 }
        harness.transport.continuation.yield(.dataReceived(from: iPad, encode(.accept(transferID: transfer.id))))
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.accept(transferID: transfer.id))))
        await eventually { harness.transport.sentResources.count == 2 }
        let iPadToken = try #require(harness.transport.sentResources.first { $0.peer == iPad }?.token)
        let macToken = try #require(harness.transport.sentResources.first { $0.peer == mac }?.token)

        harness.transport.continuation.yield(.peerStateChanged(iPad, .notConnected))
        await eventually { harness.transport.cancelledTokens.contains(iPadToken) }

        harness.transport.continuation.yield(.resourceSendFinished(token: macToken, error: nil))
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.received(transferID: transfer.id))))

        let summary = try await transfer.waitUntilCompleted()
        #expect(summary.completed.map(\.displayName) == ["Mac"])
        #expect(summary.failed.map(\.peer.displayName) == ["iPad"])
    }

    @Test("a confirmation that outruns the completion callback still completes the peer")
    func ackBeforeCompletionRace() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["Mac"])
        let mac = boxes[0]
        let file = try makeTempFile(named: "a.bin", byteCount: 10)

        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        await eventually { !harness.transport.offers(to: mac).isEmpty }
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.accept(transferID: transfer.id))))
        await eventually { harness.transport.sentResources.count == 1 }
        let token = harness.transport.sentResources[0].token

        // The receiver's "received" ack arrives before MPC's own completion
        // handler fires — a real interleaving on the wire.
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.received(transferID: transfer.id))))
        harness.transport.continuation.yield(.resourceSendFinished(token: token, error: nil))

        let summary = try await transfer.waitUntilCompleted()
        #expect(summary.completed.map(\.displayName) == ["Mac"])
    }

    @Test("cancelling a transfer notifies peers and fails remaining sends")
    func senderCancel() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["Mac"])
        let mac = boxes[0]
        let file = try makeTempFile(named: "a.bin", byteCount: 100)

        let transfer = harness.sync.send(contentsOf: file, to: .allConnectedPeers)
        await eventually { !harness.transport.offers(to: mac).isEmpty }
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.accept(transferID: transfer.id))))
        await eventually { harness.transport.sentResources.count == 1 }
        let token = harness.transport.sentResources[0].token

        await transfer.cancel()

        #expect(harness.transport.cancelledTokens.contains(token))
        let cancelSent = harness.transport.controlMessages().contains { message, peers in
            if case .cancel(let id) = message { return id == transfer.id && peers == [mac] }
            return false
        }
        #expect(cancelSent)
        await #expect(throws: LocalSyncError.self) {
            try await transfer.waitUntilCompleted()
        }
    }

    @Test("cancel that lands before the transfer's start task still cancels it")
    func cancelBeforeStart() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["Mac"])
        let file = try makeTempFile(named: "a.bin", byteCount: 10)

        // Drive the engine directly to force the race deterministically:
        // cancel arrives before startOutgoingTransfer runs.
        let id = UUID()
        let engine = harness.sync.engine
        await engine.cancelOutgoingTransfer(id: id)
        await engine.startOutgoingTransfer(
            id: id,
            source: .file(file),
            contentID: file.absoluteString,
            target: .allConnectedPeers
        )

        let transfer = Transfer(id: id, engine: engine)
        await #expect(throws: LocalSyncError.self) {
            try await transfer.waitUntilCompleted()
        }
        // No offer was ever sent to the peer.
        #expect(harness.transport.offers(to: boxes[0]).isEmpty)
    }

    // MARK: Photo export path

    @Test("a Live Photo exports two items and sends them sequentially per peer")
    func livePhotoTwoItems() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["Mac"])
        let mac = boxes[0]

        let stillMeta = TransferItemMetadata(itemID: UUID(), filename: "IMG_0001.HEIC", typeIdentifier: "public.heic", photoResourceType: 1, byteCount: 60)
        let videoMeta = TransferItemMetadata(itemID: UUID(), filename: "IMG_0001.MOV", typeIdentifier: "com.apple.quicktime-movie", photoResourceType: 9, byteCount: 40)
        harness.exporter.setResult(.success(ExportedAsset(
            kind: .livePhoto,
            items: [
                PreparedItem(metadata: stillMeta, fileURL: URL(fileURLWithPath: "/tmp/still")),
                PreparedItem(metadata: videoMeta, fileURL: URL(fileURLWithPath: "/tmp/video")),
            ]
        )), progressSteps: [0.5, 1.0])

        let transfer = await sendPhotoStub(harness, identifier: "ASSET-1", to: .allConnectedPeers)

        await eventually { !harness.transport.offers(to: mac).isEmpty }
        let offer = try #require(harness.transport.offers(to: mac).first)
        #expect(offer.kind == .livePhoto)
        #expect(offer.items.count == 2)
        #expect(offer.totalByteCount == 100)
        #expect(harness.exporter.exportedIdentifiers == ["ASSET-1"])

        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.accept(transferID: transfer.id))))

        // Items go one at a time: the second starts only after the first ends.
        await eventually { harness.transport.sentResources.count == 1 }
        #expect(harness.transport.sentResources.count == 1)
        let firstToken = harness.transport.sentResources[0].token
        harness.transport.continuation.yield(.resourceSendProgressed(token: firstToken, fraction: 0.5, completedBytes: 30, totalBytes: 60))
        harness.transport.continuation.yield(.resourceSendFinished(token: firstToken, error: nil))

        await eventually { harness.transport.sentResources.count == 2 }
        let secondToken = harness.transport.sentResources[1].token
        harness.transport.continuation.yield(.resourceSendFinished(token: secondToken, error: nil))
        harness.transport.continuation.yield(.dataReceived(from: mac, encode(.received(transferID: transfer.id))))

        let summary = try await transfer.waitUntilCompleted()
        #expect(summary.completed.count == 1)
    }

    @Test("export failure fails the transfer with a photo error")
    func exportFailure() async throws {
        let harness = Harness.make()
        _ = try await connectPeers(harness, ["Mac"])
        harness.exporter.setResult(.failure(LocalSyncError.iCloudOriginalUnavailable("offline")))

        let transfer = await sendPhotoStub(harness, identifier: "ASSET-2", to: .allConnectedPeers)
        await #expect(throws: LocalSyncError.self) {
            try await transfer.waitUntilCompleted()
        }
        #expect(harness.transport.sentResources.isEmpty)
    }

    @Test("preparation progress streams the export fractions")
    func preparationProgress() async throws {
        let harness = Harness.make()
        _ = try await connectPeers(harness, ["Mac"])
        harness.exporter.setResult(.success(ExportedAsset(
            kind: .photo,
            items: [PreparedItem(
                metadata: TransferItemMetadata(itemID: UUID(), filename: "a.jpg", typeIdentifier: nil, photoResourceType: nil, byteCount: 10),
                fileURL: URL(fileURLWithPath: "/tmp/a.jpg")
            )]
        )), progressSteps: [0.25, 0.75])

        let transfer = await sendPhotoStub(harness, identifier: "ASSET-3", to: .allConnectedPeers)
        let fractions = CaptureBox<Double>()
        let stream = await transfer.preparationProgress()
        for await fraction in stream {
            fractions.append(fraction)
        }
        // The stream always ends with 1.0; scripted steps may race the
        // subscription, but monotonic ordering must hold.
        let values = fractions.values
        #expect(values.last == 1.0)
        #expect(values == values.sorted())
    }

    // MARK: Incoming transfers

    @Test("an incoming offer surfaces, and accepting stores the files and confirms")
    func incomingHappyPath() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["iPhone"])
        let iPhone = boxes[0]

        let incomingStream = harness.sync.incomingTransfers()
        let transferID = UUID()
        let itemID = UUID()
        let offer = OfferPayload(
            transferID: transferID,
            contentID: "ASSET-9",
            kind: .photo,
            items: [TransferItemMetadata(itemID: itemID, filename: "IMG_0009.HEIC", typeIdentifier: "public.heic", photoResourceType: 1, byteCount: 50)],
            totalByteCount: 50
        )
        harness.transport.continuation.yield(.dataReceived(from: iPhone, encode(.offer(offer))))

        var iterator = incomingStream.makeAsyncIterator()
        let incoming = try #require(await iterator.next())
        #expect(incoming.from.displayName == "iPhone")
        #expect(incoming.items.first?.filename == "IMG_0009.HEIC")
        #expect(incoming.totalByteCount == 50)

        let destination = FileManager.default.temporaryDirectory
            .appendingPathComponent("lfs-incoming-\(UUID().uuidString)", isDirectory: true)
        let session = try await incoming.accept(into: destination)

        // Accept went to the sender; nothing was written yet.
        await eventually {
            harness.transport.controlMessages().contains { message, _ in
                if case .accept(let id) = message { return id == transferID }
                return false
            }
        }

        // Simulate the staged file arriving (as the transport would deliver it).
        let staged = try makeTempFile(named: "staged.bin", byteCount: 50)
        let wireName = WireFormat.resourceName(transferID: transferID, itemID: itemID, filename: "IMG_0009.HEIC")
        harness.transport.continuation.yield(.resourceReceiveProgressed(from: iPhone, name: wireName, fraction: 0.6, completedBytes: 30, totalBytes: 50))
        harness.transport.continuation.yield(.resourceReceiveFinished(from: iPhone, name: wireName, stagedURL: staged, error: nil))

        let files = try await session.files()
        #expect(files.count == 1)
        #expect(files.first?.url.lastPathComponent == "IMG_0009.HEIC")
        #expect(files.first?.url.path.hasPrefix(destination.path) == true)
        #expect(FileManager.default.fileExists(atPath: files.first?.url.path ?? ""))
        #expect(!FileManager.default.fileExists(atPath: staged.path))

        // The receiver confirmed storage back to the sender.
        let ackSent = await eventually {
            harness.transport.controlMessages().contains { message, _ in
                if case .received(let id) = message { return id == transferID }
                return false
            }
        }
        #expect(ackSent)
    }

    @Test("declining an incoming offer informs the sender and receives nothing")
    func incomingDecline() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["iPhone"])
        let iPhone = boxes[0]

        let incomingStream = harness.sync.incomingTransfers()
        let transferID = UUID()
        let offer = OfferPayload(
            transferID: transferID,
            contentID: "ASSET-9",
            kind: .file,
            items: [TransferItemMetadata(itemID: UUID(), filename: "doc.pdf", typeIdentifier: nil, photoResourceType: nil, byteCount: 10)],
            totalByteCount: 10
        )
        harness.transport.continuation.yield(.dataReceived(from: iPhone, encode(.offer(offer))))

        var iterator = incomingStream.makeAsyncIterator()
        let incoming = try #require(await iterator.next())
        await incoming.decline()

        let declineSent = await eventually {
            harness.transport.controlMessages().contains { message, _ in
                if case .decline(let id) = message { return id == transferID }
                return false
            }
        }
        #expect(declineSent)

        // Accepting after declining fails.
        await #expect(throws: LocalSyncError.self) {
            _ = try await incoming.accept()
        }
    }

    @Test("an unsolicited resource is deleted and never surfaced")
    func unsolicitedResourceDiscarded() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["iPhone"])
        let iPhone = boxes[0]

        let staged = try makeTempFile(named: "sneaky.bin", byteCount: 10)
        let wireName = WireFormat.resourceName(transferID: UUID(), itemID: UUID(), filename: "sneaky.bin")
        harness.transport.continuation.yield(.resourceReceiveFinished(from: iPhone, name: wireName, stagedURL: staged, error: nil))

        let deleted = await eventually { !FileManager.default.fileExists(atPath: staged.path) }
        #expect(deleted)
    }

    @Test("a sender's cancel fails the accepted incoming transfer")
    func incomingSenderCancel() async throws {
        let harness = Harness.make()
        let boxes = try await connectPeers(harness, ["iPhone"])
        let iPhone = boxes[0]

        let incomingStream = harness.sync.incomingTransfers()
        let transferID = UUID()
        let offer = OfferPayload(
            transferID: transferID,
            contentID: "X",
            kind: .file,
            items: [TransferItemMetadata(itemID: UUID(), filename: "doc.pdf", typeIdentifier: nil, photoResourceType: nil, byteCount: 10)],
            totalByteCount: 10
        )
        harness.transport.continuation.yield(.dataReceived(from: iPhone, encode(.offer(offer))))

        var iterator = incomingStream.makeAsyncIterator()
        let incoming = try #require(await iterator.next())
        let session = try await incoming.accept()

        harness.transport.continuation.yield(.dataReceived(from: iPhone, encode(.cancel(transferID: transferID))))

        await #expect(throws: LocalSyncError.self) {
            _ = try await session.files()
        }
    }

    // MARK: Helpers

    /// `send(photo:to:)` needs a real PHAsset; tests reach the same engine
    /// path with a stub identifier and the fake exporter.
    private func sendPhotoStub(_ harness: Harness, identifier: String, to target: TransferTarget) async -> Transfer {
        let id = UUID()
        let engine = harness.sync.engine
        Task {
            await engine.startOutgoingTransfer(
                id: id,
                source: .photoAsset(localIdentifier: identifier),
                contentID: identifier,
                target: target
            )
        }
        return Transfer(id: id, engine: engine)
    }
}

// MARK: - Phase helpers

extension TransferPhase {
    var isWaitingForAcceptance: Bool {
        if case .waitingForAcceptance = self { return true }
        return false
    }

    var isCompleted: Bool {
        if case .completed = self { return true }
        return false
    }
}
