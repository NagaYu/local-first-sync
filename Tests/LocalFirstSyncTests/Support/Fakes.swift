import Foundation
import MultipeerConnectivity
import os
@testable import LocalFirstSync

// MARK: - FakeTransport

/// A scriptable transport: tests yield `TransportEvent`s in any interleaving
/// they want to prove safe, and inspect every call the engine made.
final class FakeTransport: MultipeerTransport, @unchecked Sendable {
    let events: AsyncStream<TransportEvent>
    /// Tests drive the engine by yielding events here — exactly what the real
    /// delegate bridge does, so interleavings reproduce faithfully.
    let continuation: AsyncStream<TransportEvent>.Continuation

    struct SentData: Sendable {
        let data: Data
        let peers: [MCPeerIDBox]
    }

    struct SentResource: Sendable {
        let url: URL
        let name: String
        let peer: MCPeerIDBox
        let token: UUID
    }

    private struct State {
        var browsing = false
        var advertising = false
        var discoveryInfo: [String: String]?
        var invites: [(peer: MCPeerIDBox, context: Data?)] = []
        var sentData: [SentData] = []
        var sentResources: [SentResource] = []
        var cancelledTokens: [UUID] = []
        var disconnectCount = 0
        var sendError: (any Error)?
    }

    private let state = OSAllocatedUnfairLock(uncheckedState: State())

    init() {
        (events, continuation) = AsyncStream.makeStream(of: TransportEvent.self, bufferingPolicy: .unbounded)
    }

    // MARK: MultipeerTransport

    func startBrowsing() throws {
        state.withLockUnchecked { $0.browsing = true }
    }

    func stopBrowsing() {
        state.withLockUnchecked { $0.browsing = false }
    }

    func startAdvertising(discoveryInfo: [String: String]) throws {
        state.withLockUnchecked {
            $0.advertising = true
            $0.discoveryInfo = discoveryInfo
        }
    }

    func stopAdvertising() {
        state.withLockUnchecked { $0.advertising = false }
    }

    func invite(_ peer: MCPeerIDBox, context: Data?, timeout: TimeInterval) {
        state.withLockUnchecked { $0.invites.append((peer, context)) }
    }

    func send(_ data: Data, to peers: [MCPeerIDBox]) throws {
        let error = state.withLockUnchecked { s -> (any Error)? in
            if s.sendError == nil {
                s.sentData.append(SentData(data: data, peers: peers))
            }
            return s.sendError
        }
        if let error { throw error }
    }

    func sendResource(at url: URL, name: String, to peer: MCPeerIDBox, token: UUID) {
        state.withLockUnchecked {
            $0.sentResources.append(SentResource(url: url, name: name, peer: peer, token: token))
        }
    }

    func cancelResourceSend(token: UUID) {
        state.withLockUnchecked { $0.cancelledTokens.append(token) }
    }

    func disconnect() {
        state.withLockUnchecked { $0.disconnectCount += 1 }
    }

    // MARK: Test inspection

    var isBrowsing: Bool { state.withLockUnchecked { $0.browsing } }
    var isAdvertising: Bool { state.withLockUnchecked { $0.advertising } }
    var advertisedDiscoveryInfo: [String: String]? { state.withLockUnchecked { $0.discoveryInfo } }
    var invites: [(peer: MCPeerIDBox, context: Data?)] { state.withLockUnchecked { $0.invites } }
    var sentData: [SentData] { state.withLockUnchecked { $0.sentData } }
    var sentResources: [SentResource] { state.withLockUnchecked { $0.sentResources } }
    var cancelledTokens: [UUID] { state.withLockUnchecked { $0.cancelledTokens } }
    var disconnectCount: Int { state.withLockUnchecked { $0.disconnectCount } }

    func setSendError(_ error: (any Error)?) {
        state.withLockUnchecked { $0.sendError = error }
    }

    /// Every decoded control message sent so far, with its target peers.
    func controlMessages() -> [(message: ControlMessage, peers: [MCPeerIDBox])] {
        sentData.compactMap { sent in
            guard let message = try? JSONDecoder().decode(ControlMessage.self, from: sent.data) else { return nil }
            return (message, sent.peers)
        }
    }

    /// The offers sent to the given peer.
    func offers(to peer: MCPeerIDBox) -> [OfferPayload] {
        controlMessages().compactMap { message, peers in
            guard peers.contains(peer), case .offer(let payload) = message else { return nil }
            return payload
        }
    }
}

// MARK: - FakeExporter

/// Stands in for PhotoKit export: returns preconfigured items and replays
/// scripted preparation-progress values.
final class FakeExporter: AssetExporting, @unchecked Sendable {
    private struct State {
        var result: Result<ExportedAsset, any Error>?
        var progressSteps: [Double] = []
        var exportedIdentifiers: [String] = []
    }

    private let state = OSAllocatedUnfairLock(uncheckedState: State())

    func setResult(_ result: Result<ExportedAsset, any Error>, progressSteps: [Double] = []) {
        state.withLockUnchecked {
            $0.result = result
            $0.progressSteps = progressSteps
        }
    }

    var exportedIdentifiers: [String] { state.withLockUnchecked { $0.exportedIdentifiers } }

    func export(
        assetIdentifier: String,
        into directory: URL,
        progress: @escaping @Sendable (Double) -> Void
    ) async throws -> ExportedAsset {
        let (result, steps) = state.withLockUnchecked { s -> (Result<ExportedAsset, any Error>?, [Double]) in
            s.exportedIdentifiers.append(assetIdentifier)
            return (s.result, s.progressSteps)
        }
        for step in steps {
            progress(step)
        }
        guard let result else {
            throw LocalSyncError.assetExportFailed("FakeExporter was not configured")
        }
        return try result.get()
    }
}

// MARK: - Harness

/// One LocalSync wired to fakes, ready for scripted-event tests.
struct Harness {
    let sync: LocalSync
    let transport: FakeTransport
    let exporter: FakeExporter

    static func make(
        serviceName: String = "test-sync",
        configuration: LocalSyncConfiguration = LocalSyncConfiguration(),
        stableID: String = "local-stable-id"
    ) -> Harness {
        let transport = FakeTransport()
        let exporter = FakeExporter()
        let sync = LocalSync(
            serviceName: serviceName,
            displayName: "Test Device",
            configuration: configuration,
            transport: transport,
            exporter: exporter,
            stableID: stableID
        )
        return Harness(sync: sync, transport: transport, exporter: exporter)
    }
}

// MARK: - Helpers

/// Connects the named peers to the harness's session and returns their boxes.
func connectPeers(_ harness: Harness, _ names: [String]) async throws -> [MCPeerIDBox] {
    var boxes: [MCPeerIDBox] = []
    for name in names {
        let box = makePeerBox(name)
        let task = Task { try await harness.sync.connect(to: Peer(box: box)) }
        await eventually { harness.transport.invites.contains { $0.peer == box } }
        harness.transport.continuation.yield(.peerStateChanged(box, .connected))
        try await task.value
        boxes.append(box)
    }
    return boxes
}

/// Creates a real temporary file of `byteCount` bytes and returns its URL.
func makeTempFile(named name: String, byteCount: Int) throws -> URL {
    let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("lfs-tests-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    let url = directory.appendingPathComponent(name, isDirectory: false)
    try Data(repeating: 0xAB, count: byteCount).write(to: url)
    return url
}

func makePeerBox(_ name: String) -> MCPeerIDBox {
    MCPeerIDBox(MCPeerID(displayName: name))
}

func makePeer(_ name: String, stableID: String? = nil) -> Peer {
    Peer(box: makePeerBox(name), stableID: stableID)
}

/// Polls until `condition` holds or the timeout passes. The engine consumes
/// events asynchronously, so tests await observable effects instead of
/// assuming synchronous processing.
@discardableResult
func eventually(
    timeout: TimeInterval = 2,
    _ condition: @Sendable () async -> Bool
) async -> Bool {
    let deadline = ContinuousClock.now.advanced(by: .seconds(timeout))
    while ContinuousClock.now < deadline {
        if await condition() { return true }
        try? await Task.sleep(nanoseconds: 5_000_000)
    }
    return await condition()
}

/// Encodes a control message the way a remote peer would.
func encode(_ message: ControlMessage) -> Data {
    (try? JSONEncoder().encode(message)) ?? Data()
}

/// A capture box for values produced inside @Sendable closures.
final class CaptureBox<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [Value] = []

    func append(_ value: Value) {
        lock.lock()
        defer { lock.unlock() }
        storage.append(value)
    }

    var values: [Value] {
        lock.lock()
        defer { lock.unlock() }
        return storage
    }
}
