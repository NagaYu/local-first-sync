import Foundation
import Testing
@testable import LocalFirstSync

/// Discovery, connection, and invitation flows — the delegate→async bridging
/// core. The one-connected-peer case (a two-device household) is the primary
/// scenario; three simultaneous peers must go through the same code path
/// without regressing it.
@Suite("Connection & bridging")
struct ConnectionTests {
    // MARK: Baseline: one peer (the two-device household)

    @Test("connect(to:) resolves when the session reports the peer connected")
    func connectSinglePeer() async throws {
        let harness = Harness.make()
        let box = makePeerBox("Mac")
        let peer = Peer(box: box)

        let connectTask = Task { try await harness.sync.connect(to: peer) }

        // The engine must have sent the invitation before we simulate success.
        let invited = await eventually { harness.transport.invites.contains { $0.peer == box } }
        #expect(invited)

        // The invitation context carries our hello payload for stable identity.
        let context = harness.transport.invites.first?.context
        let hello = context.flatMap { try? JSONDecoder().decode(HelloPayload.self, from: $0) }
        #expect(hello?.stableID == "local-stable-id")

        harness.transport.continuation.yield(.peerStateChanged(box, .connected))
        try await connectTask.value

        let connected = await harness.sync.connectedPeers
        #expect(connected.map(\.displayName) == ["Mac"])
    }

    @Test("connect(to:) throws when the session reports the connection failed")
    func connectFailure() async {
        let harness = Harness.make()
        let box = makePeerBox("Mac")
        let peer = Peer(box: box)

        let connectTask = Task { try await harness.sync.connect(to: peer) }
        await eventually { !harness.transport.invites.isEmpty }
        harness.transport.continuation.yield(.peerStateChanged(box, .notConnected))

        await #expect(throws: LocalSyncError.self) {
            try await connectTask.value
        }
        let connected = await harness.sync.connectedPeers
        #expect(connected.isEmpty)
    }

    @Test("connect(to:) times out when nothing answers")
    func connectTimeout() async {
        var configuration = LocalSyncConfiguration()
        configuration.connectTimeout = 0.15
        let harness = Harness.make(configuration: configuration)
        let peer = makePeer("Mac")

        await #expect(throws: LocalSyncError.self) {
            try await harness.sync.connect(to: peer)
        }
    }

    @Test("cancelling connect(to:) resumes exactly once; a late connected event still lands safely")
    func connectCancellation() async {
        let harness = Harness.make()
        let box = makePeerBox("Mac")
        let peer = Peer(box: box)

        let connectTask = Task { try await harness.sync.connect(to: peer) }
        await eventually { !harness.transport.invites.isEmpty }
        connectTask.cancel()

        await #expect(throws: (any Error).self) {
            try await connectTask.value
        }

        // A late .connected must not double-resume (CheckedContinuation would
        // trap) and the state change still applies.
        harness.transport.continuation.yield(.peerStateChanged(box, .connected))
        let connected = await eventually {
            await harness.sync.connectedPeers.contains(peer)
        }
        #expect(connected)
    }

    @Test("connecting to an already-connected peer returns immediately")
    func connectIdempotent() async throws {
        let harness = Harness.make()
        let box = makePeerBox("Mac")
        let peer = Peer(box: box)

        let first = Task { try await harness.sync.connect(to: peer) }
        await eventually { !harness.transport.invites.isEmpty }
        harness.transport.continuation.yield(.peerStateChanged(box, .connected))
        try await first.value

        // No new invitation needed; resolves without any event.
        try await harness.sync.connect(to: peer)
        #expect(harness.transport.invites.count == 1)
    }

    // MARK: Three peers — must not regress the baseline

    @Test("three simultaneous connects resolve independently under interleaved events")
    func threePeersInterleaved() async throws {
        let harness = Harness.make()
        let boxes = ["iPhone", "iPad", "Mac"].map(makePeerBox)
        let peers = boxes.map { Peer(box: $0) }

        let tasks = peers.map { peer in
            Task { try await harness.sync.connect(to: peer) }
        }
        await eventually { harness.transport.invites.count == 3 }

        // Deliberately interleaved: B connects between A's connecting and
        // connected, C's connected arrives while A and B already resolved.
        harness.transport.continuation.yield(.peerStateChanged(boxes[0], .connecting))
        harness.transport.continuation.yield(.peerStateChanged(boxes[1], .connected))
        harness.transport.continuation.yield(.peerStateChanged(boxes[0], .connected))
        harness.transport.continuation.yield(.peerStateChanged(boxes[2], .connecting))
        harness.transport.continuation.yield(.peerStateChanged(boxes[2], .connected))

        for task in tasks {
            try await task.value
        }
        let connected = await harness.sync.connectedPeers
        #expect(Set(connected.map(\.displayName)) == ["iPhone", "iPad", "Mac"])
    }

    @Test("one peer's connection failure never resumes another peer's connect")
    func failureIsolationAcrossPeers() async throws {
        let harness = Harness.make()
        let boxA = makePeerBox("iPad")
        let boxB = makePeerBox("Mac")

        let taskA = Task { try await harness.sync.connect(to: Peer(box: boxA)) }
        let taskB = Task { try await harness.sync.connect(to: Peer(box: boxB)) }
        await eventually { harness.transport.invites.count == 2 }

        // B fails first; A must stay pending and then succeed.
        harness.transport.continuation.yield(.peerStateChanged(boxB, .notConnected))
        await #expect(throws: LocalSyncError.self) {
            try await taskB.value
        }

        harness.transport.continuation.yield(.peerStateChanged(boxA, .connected))
        try await taskA.value

        let connected = await harness.sync.connectedPeers
        #expect(connected.map(\.displayName) == ["iPad"])
    }

    @Test("a disconnect of one connected peer leaves the others connected")
    func disconnectIsolation() async throws {
        let harness = Harness.make()
        let boxes = ["iPhone", "iPad", "Mac"].map(makePeerBox)
        for box in boxes {
            let task = Task { try await harness.sync.connect(to: Peer(box: box)) }
            await eventually { harness.transport.invites.contains { $0.peer == box } }
            harness.transport.continuation.yield(.peerStateChanged(box, .connected))
            try await task.value
        }

        harness.transport.continuation.yield(.peerStateChanged(boxes[1], .notConnected))
        await eventually { await harness.sync.connectedPeers.count == 2 }
        let connected = await harness.sync.connectedPeers
        #expect(Set(connected.map(\.displayName)) == ["iPhone", "Mac"])
    }

    // MARK: Discovery

    @Test("nearbyPeers yields found peers, replays them to late subscribers, and carries stable IDs")
    func discoveryStream() async {
        let harness = Harness.make()
        let box = makePeerBox("iPad")

        let firstStream = harness.sync.nearbyPeers()
        await eventually { harness.transport.isBrowsing }
        harness.transport.continuation.yield(.peerFound(box, discoveryInfo: [DiscoveryInfoKey.stableID: "ipad-id"]))

        var iterator = firstStream.makeAsyncIterator()
        let found = await iterator.next()
        #expect(found?.displayName == "iPad")
        #expect(found?.stableID == "ipad-id")

        // A late subscriber sees the already-discovered peer replayed.
        var lateIterator = harness.sync.nearbyPeers().makeAsyncIterator()
        let replayed = await lateIterator.next()
        #expect(replayed?.displayName == "iPad")
    }

    @Test("events reach every subscriber, not just the first")
    func multiConsumerEvents() async {
        let harness = Harness.make()
        let box = makePeerBox("Mac")

        let capturedA = CaptureBox<SyncEvent>()
        let capturedB = CaptureBox<SyncEvent>()
        let streamA = harness.sync.events()
        let streamB = harness.sync.events()
        let taskA = Task { for await event in streamA { capturedA.append(event) } }
        let taskB = Task { for await event in streamB { capturedB.append(event) } }

        // Give both subscribers time to register before the event fires.
        try? await Task.sleep(nanoseconds: 50_000_000)
        harness.transport.continuation.yield(.peerFound(box, discoveryInfo: nil))

        let bothSaw = await eventually {
            capturedA.values.count == 1 && capturedB.values.count == 1
        }
        #expect(bothSaw)
        taskA.cancel()
        taskB.cancel()
    }

    // MARK: Invitations — no silent trust

    @Test("an invitation is surfaced and connects only after an explicit accept")
    func invitationAccept() async throws {
        let harness = Harness.make()
        let box = makePeerBox("iPhone")
        let responses = CaptureBox<Bool>()
        let responder = InvitationResponder { responses.append($0) }
        let context = try JSONEncoder().encode(HelloPayload(stableID: "iphone-id", displayName: "iPhone", protocolVersion: 1))

        let requests = harness.sync.connectionRequests()
        await eventually { harness.transport.isAdvertising }
        harness.transport.continuation.yield(.invitationReceived(box, context: context, respond: responder))

        var iterator = requests.makeAsyncIterator()
        let request = await iterator.next()
        #expect(request?.from.displayName == "iPhone")
        #expect(request?.from.stableID == "iphone-id")

        // Nothing was answered automatically.
        #expect(responses.values.isEmpty)

        let acceptTask = Task { try await request?.accept() }
        await eventually { responses.values == [true] }
        harness.transport.continuation.yield(.peerStateChanged(box, .connected))
        try await acceptTask.value

        let connected = await harness.sync.connectedPeers
        #expect(connected.map(\.displayName) == ["iPhone"])
    }

    @Test("declining an invitation answers false and connects nothing")
    func invitationDecline() async {
        let harness = Harness.make()
        let box = makePeerBox("iPhone")
        let responses = CaptureBox<Bool>()
        let responder = InvitationResponder { responses.append($0) }

        let requests = harness.sync.connectionRequests()
        await eventually { harness.transport.isAdvertising }
        harness.transport.continuation.yield(.invitationReceived(box, context: nil, respond: responder))

        var iterator = requests.makeAsyncIterator()
        let request = await iterator.next()
        await request?.decline()

        #expect(responses.values == [false])
        let connected = await harness.sync.connectedPeers
        #expect(connected.isEmpty)
    }

    @Test("answering the same invitation twice fails the second answer")
    func invitationDoubleResponse() async throws {
        let harness = Harness.make()
        let box = makePeerBox("iPhone")
        let responses = CaptureBox<Bool>()
        let responder = InvitationResponder { responses.append($0) }

        let requests = harness.sync.connectionRequests()
        await eventually { harness.transport.isAdvertising }
        harness.transport.continuation.yield(.invitationReceived(box, context: nil, respond: responder))

        var iterator = requests.makeAsyncIterator()
        let request = try #require(await iterator.next())
        await request.decline()

        await #expect(throws: LocalSyncError.self) {
            try await request.accept()
        }
        // The framework handler itself was only ever called once.
        #expect(responses.values == [false])
    }

    @Test("InvitationResponder calls the underlying handler at most once under racing responses")
    func responderCallOnce() async {
        let responses = CaptureBox<Bool>()
        let responder = InvitationResponder { responses.append($0) }

        await withTaskGroup(of: Void.self) { group in
            for index in 0..<16 {
                group.addTask { responder.respond(index.isMultiple(of: 2)) }
            }
        }
        #expect(responses.values.count == 1)
    }

    // MARK: Hello exchange

    @Test("a hello message upgrades the connected peer's stable identity")
    func helloUpdatesStableID() async throws {
        let harness = Harness.make()
        let box = makePeerBox("Mac")
        let peer = Peer(box: box)

        let task = Task { try await harness.sync.connect(to: peer) }
        await eventually { !harness.transport.invites.isEmpty }
        harness.transport.continuation.yield(.peerStateChanged(box, .connected))
        try await task.value

        harness.transport.continuation.yield(.dataReceived(
            from: box,
            encode(.hello(HelloPayload(stableID: "mac-stable", displayName: "Mac", protocolVersion: 1)))
        ))

        let upgraded = await eventually {
            await harness.sync.connectedPeers.first?.stableID == "mac-stable"
        }
        #expect(upgraded)
    }

    @Test("connecting sends our hello to the new peer")
    func helloSentOnConnect() async throws {
        let harness = Harness.make()
        let box = makePeerBox("Mac")

        let task = Task { try await harness.sync.connect(to: Peer(box: box)) }
        await eventually { !harness.transport.invites.isEmpty }
        harness.transport.continuation.yield(.peerStateChanged(box, .connected))
        try await task.value

        let helloSent = await eventually {
            harness.transport.controlMessages().contains { message, peers in
                if case .hello(let payload) = message {
                    return payload.stableID == "local-stable-id" && peers == [box]
                }
                return false
            }
        }
        #expect(helloSent)
    }

    // MARK: Service-name validation

    @Test("invalid service names are rejected with a reason")
    func serviceNameValidation() {
        #expect(throws: LocalSyncError.self) { try LocalSync.validateServiceName("") }
        #expect(throws: LocalSyncError.self) { try LocalSync.validateServiceName("way-too-long-name-x") }
        #expect(throws: LocalSyncError.self) { try LocalSync.validateServiceName("UPPER") }
        #expect(throws: LocalSyncError.self) { try LocalSync.validateServiceName("-lead") }
        #expect(throws: LocalSyncError.self) { try LocalSync.validateServiceName("trail-") }
        #expect(throws: LocalSyncError.self) { try LocalSync.validateServiceName("a--b") }
        #expect(throws: LocalSyncError.self) { try LocalSync.validateServiceName("12345") }
        #expect(throws: Never.self) { try LocalSync.validateServiceName("my-app-sync") }
        #expect(throws: Never.self) { try LocalSync.validateServiceName("abc123") }
    }

    @Test("startDiscovery surfaces an invalid service name as an error")
    func invalidServiceNameSurfaced() async {
        let harness = Harness.make(serviceName: "Bad Name!")
        await #expect(throws: LocalSyncError.self) {
            try await harness.sync.startDiscovery()
        }
        #expect(!harness.transport.isBrowsing)
    }
}
