import Foundation
import MultipeerConnectivity

#if canImport(UIKit)
import UIKit
#endif

// MARK: - LocalSync

/// Syncs photos and files across a user's own nearby Apple devices — iPhone,
/// iPad, and Mac in one session at once — over Multipeer Connectivity.
/// No cloud, no server, no account.
///
/// One `LocalSync` owns one encrypted `MCSession` that can hold multiple
/// simultaneously connected peers (the framework supports up to 8, including
/// this device). Connecting a second, third, or fourth device goes through
/// the same code path — nothing here assumes exactly two devices.
///
/// ```swift
/// let sync = LocalSync(serviceName: "my-app-sync")
///
/// for await peer in sync.nearbyPeers() {
///     // show peer.displayName; let the user pick
/// }
/// try await sync.connect(to: peer)
///
/// for await update in sync.send(photo: asset, to: .allConnectedPeers) {
///     // update.peer, update.progress.fractionCompleted — per peer, never blended
/// }
///
/// for await incoming in sync.incomingTransfers() {
///     // incoming.from, try await incoming.accept() / await incoming.decline()
/// }
/// ```
public final class LocalSync: Sendable {
    let engine: SyncEngine
    private let pump: Task<Void, Never>

    /// The service name this instance advertises and browses for.
    public let serviceName: String
    /// The name other devices see for this device.
    public let localDisplayName: String

    /// Creates a sync session for the given service.
    ///
    /// - Parameters:
    ///   - serviceName: A Bonjour-style service type unique to your app
    ///     (1–15 characters; lowercase ASCII letters, digits, and hyphens; at
    ///     least one letter; no leading/trailing/adjacent hyphens). Invalid
    ///     names are reported by the throwing methods
    ///     (``startDiscovery()``, ``startAdvertising()``) and by
    ///     ``validateServiceName(_:)``.
    ///   - displayName: The name shown to other devices. Defaults to the
    ///     device name.
    ///   - configuration: Timeout tuning. Defaults are sensible.
    public convenience init(
        serviceName: String,
        displayName: String? = nil,
        configuration: LocalSyncConfiguration = LocalSyncConfiguration()
    ) {
        let name = displayName ?? Self.defaultDisplayName()
        let transport = MCTransport(displayName: name, serviceType: serviceName)
        self.init(
            serviceName: serviceName,
            displayName: name,
            configuration: configuration,
            transport: transport,
            exporter: PhotoAssetExporter(),
            stableID: Self.persistedStableID(for: serviceName)
        )
    }

    /// Internal designated initializer; tests inject a fake transport and exporter.
    init(
        serviceName: String,
        displayName: String,
        configuration: LocalSyncConfiguration,
        transport: any MultipeerTransport,
        exporter: any AssetExporting,
        stableID: String
    ) {
        self.serviceName = serviceName
        self.localDisplayName = displayName
        let engine = SyncEngine(
            serviceName: serviceName,
            localDisplayName: displayName,
            stableID: stableID,
            configuration: configuration,
            transport: transport,
            exporter: exporter
        )
        self.engine = engine
        self.pump = Task { await engine.run() }
    }

    deinit {
        pump.cancel()
    }

    // MARK: Discovery

    /// Starts looking for nearby devices advertising the same service.
    /// - Throws: ``LocalSyncError/invalidServiceName(_:reason:)``
    public func startDiscovery() async throws {
        try await engine.startBrowsing()
    }

    /// Stops browsing for nearby devices.
    public func stopDiscovery() async {
        await engine.stopBrowsing()
    }

    /// Makes this device visible to nearby devices browsing for the same
    /// service. Being visible never grants access: every connection and every
    /// transfer still requires an explicit accept on this side.
    /// - Throws: ``LocalSyncError/invalidServiceName(_:reason:)``
    public func startAdvertising() async throws {
        try await engine.startAdvertising()
    }

    /// Stops advertising this device.
    public func stopAdvertising() async {
        await engine.stopAdvertising()
    }

    /// Nearby devices advertising the same service, as they are found.
    ///
    /// Starts browsing if it isn't already started. Devices already
    /// discovered are replayed first, so late iterators see the full picture.
    /// If the service name is invalid the stream finishes immediately —
    /// call ``startDiscovery()`` to receive the error instead.
    public func nearbyPeers() -> AsyncStream<Peer> {
        bridge { engine in await engine.nearbyPeersStream() }
    }

    /// Incoming connection requests from nearby devices, each awaiting an
    /// explicit ``ConnectionRequest/accept()`` or ``ConnectionRequest/decline()``.
    ///
    /// Starts advertising if it isn't already started. Requests are never
    /// accepted automatically; an unanswered request times out on the
    /// requesting side as if declined.
    public func connectionRequests() -> AsyncStream<ConnectionRequest> {
        bridge { engine in await engine.connectionRequestsStream() }
    }

    /// Session-level events: peers found, lost, connected, disconnected.
    public func events() -> AsyncStream<SyncEvent> {
        bridge { engine in await engine.eventsStream() }
    }

    // MARK: Connection

    /// Peers currently connected to this session.
    public var connectedPeers: [Peer] {
        get async { await engine.currentConnectedPeers() }
    }

    /// Invites a discovered peer into this device's session and waits until
    /// the connection is established.
    ///
    /// Call once per additional device — an iPhone, iPad, and Mac can all be
    /// connected to the same session at once. The peer's user must accept the
    /// connection request on their side.
    public func connect(to peer: Peer) async throws {
        try await engine.connect(to: peer)
    }

    /// Leaves the session and stops discovery and advertising.
    public func disconnect() async {
        await engine.shutdown()
    }

    // MARK: Sync-state tracking

    /// Attaches a SwiftData-backed store that records, per (content, peer)
    /// pair, whether that specific peer has confirmed receipt. "Synced with
    /// the Mac" and "synced with the iPad" are tracked as independent facts.
    public func attachSyncState(_ store: SyncStateStore) async {
        await engine.attachSyncState(store)
    }

    // MARK: Helpers

    /// Wraps an actor-produced stream in a synchronously-returnable one.
    private func bridge<Element: Sendable>(
        _ makeInner: @escaping @Sendable (SyncEngine) async -> AsyncStream<Element>
    ) -> AsyncStream<Element> {
        let engine = self.engine
        let (stream, continuation) = AsyncStream.makeStream(of: Element.self)
        let task = Task {
            let inner = await makeInner(engine)
            for await element in inner {
                continuation.yield(element)
            }
            continuation.finish()
        }
        continuation.onTermination = { _ in task.cancel() }
        return stream
    }

    private static func defaultDisplayName() -> String {
        #if canImport(UIKit)
        UIDevice.current.name
        #else
        Host.current().localizedName ?? ProcessInfo.processInfo.hostName
        #endif
    }

    /// A per-install identity, persisted so sync state keyed on it survives
    /// app relaunches. Never leaves the local network (it travels only inside
    /// discovery info, invitation context, and hello messages between peers).
    private static func persistedStableID(for serviceName: String) -> String {
        let key = "LocalFirstSync.peerIdentity.\(serviceName)"
        let defaults = UserDefaults.standard
        if let existing = defaults.string(forKey: key) {
            return existing
        }
        let fresh = UUID().uuidString
        defaults.set(fresh, forKey: key)
        return fresh
    }
}

// MARK: - ConnectionRequest

/// A nearby device asking to join this device's session. Nothing connects
/// until ``accept()`` is called — mirroring the AirDrop pattern users already
/// understand.
public struct ConnectionRequest: Sendable, Identifiable {
    /// Identifies this request.
    public let id: UUID
    /// The device asking to connect.
    public let from: Peer

    let engine: SyncEngine

    /// Accepts the request and waits until the peer is connected.
    /// - Throws: ``LocalSyncError/alreadyResponded`` if already answered,
    ///   or a connection error if establishing the link fails.
    public func accept() async throws {
        try await engine.respondToInvitation(id: id, accept: true)
    }

    /// Declines the request. The requesting side sees its connection attempt
    /// fail.
    public func decline() async {
        try? await engine.respondToInvitation(id: id, accept: false)
    }
}

// MARK: - Hello payload

/// Exchanged when peers meet: in discovery info (abbreviated), invitation
/// context, and as the first control message after connecting. Carries the
/// stable per-install identity that per-peer sync state is keyed on.
struct HelloPayload: Codable, Sendable {
    var stableID: String
    var displayName: String
    var protocolVersion: Int
}

// MARK: - SyncEngine

/// The single place where all Multipeer state lives and mutates.
///
/// Every transport event is handled in order by one `for await` loop
/// (``run()``), so a callback about peer B can never interleave with
/// state changes for peer A: both are just events in one serialized mailbox.
/// Continuations for `connect(to:)` are keyed per peer and resolved
/// take-then-resume, so completion, timeout, and cancellation race safely.
actor SyncEngine {
    // MARK: Dependencies

    let serviceName: String
    let localDisplayName: String
    let localStableID: String
    let configuration: LocalSyncConfiguration
    let transport: any MultipeerTransport
    let exporter: any AssetExporting
    private(set) var syncStore: SyncStateStore?

    // MARK: Peer state

    private(set) var discovered: [MCPeerIDBox: Peer] = [:]
    private(set) var connected: [MCPeerIDBox: Peer] = [:]

    private var browsingStarted = false
    private var advertisingStarted = false

    // MARK: Connect continuations (keyed per peer, take-then-resume)

    private struct PendingConnect {
        var continuations: [UUID: CheckedContinuation<Void, any Error>] = [:]
        var watchdogs: [UUID: Task<Void, Never>] = [:]
    }

    private var pendingConnects: [MCPeerIDBox: PendingConnect] = [:]

    // MARK: Pending invitations

    struct PendingInvitation {
        let peer: Peer
        let responder: InvitationResponder
        var responded = false
    }

    private var pendingInvitations: [UUID: PendingInvitation] = [:]

    // MARK: Subscribers

    private var peerSubscribers: [UUID: AsyncStream<Peer>.Continuation] = [:]
    private var eventSubscribers: [UUID: AsyncStream<SyncEvent>.Continuation] = [:]
    private var requestSubscribers: [UUID: AsyncStream<ConnectionRequest>.Continuation] = [:]
    var incomingSubscribers: [UUID: AsyncStream<IncomingTransfer>.Continuation] = [:]

    // MARK: Transfer state (managed in Transfer.swift extensions)

    var outgoingTransfers: [UUID: OutgoingTransferState] = [:]
    var completedSummaries: [UUID: TransferSummary] = [:]
    var incomingOffers: [UUID: IncomingOfferState] = [:]
    /// Maps in-flight `sendResource` tokens back to (transfer, peer).
    var resourceTokens: [UUID: (transferID: UUID, box: MCPeerIDBox)] = [:]
    /// Serializes sync-store writes so state transitions land in order.
    var storeWriteChain: Task<Void, Never>?

    init(
        serviceName: String,
        localDisplayName: String,
        stableID: String,
        configuration: LocalSyncConfiguration,
        transport: any MultipeerTransport,
        exporter: any AssetExporting
    ) {
        self.serviceName = serviceName
        self.localDisplayName = localDisplayName
        self.localStableID = stableID
        self.configuration = configuration
        self.transport = transport
        self.exporter = exporter
    }

    // MARK: Event pump

    /// Consumes the transport's event mailbox — the only reader. Sequential
    /// processing here is what makes multi-peer interleavings safe.
    func run() async {
        for await event in transport.events {
            if Task.isCancelled { break }
            await handle(event)
        }
    }

    private func handle(_ event: TransportEvent) async {
        switch event {
        case .peerFound(let box, let discoveryInfo):
            let peer = Peer(box: box, stableID: discoveryInfo?[DiscoveryInfoKey.stableID])
            discovered[box] = peer
            yieldEvent(.peerFound(peer))
            for continuation in peerSubscribers.values {
                continuation.yield(peer)
            }

        case .peerLost(let box):
            if let peer = discovered.removeValue(forKey: box) {
                yieldEvent(.peerLost(peer))
            }

        case .peerStateChanged(let box, let state):
            handlePeerStateChange(box, state)

        case .invitationReceived(let box, let context, let responder):
            handleInvitation(box, context: context, responder: responder)

        case .dataReceived(let from, let data):
            await handleControlData(from: from, data: data)

        case .resourceSendProgressed(let token, let fraction, let completedBytes, let totalBytes):
            handleResourceSendProgress(token: token, fraction: fraction, completedBytes: completedBytes, totalBytes: totalBytes)

        case .resourceSendFinished(let token, let error):
            await handleResourceSendFinished(token: token, error: error)

        case .resourceReceiveStarted:
            break // Progress events carry everything the engine needs.

        case .resourceReceiveProgressed(let from, let name, let fraction, let completedBytes, let totalBytes):
            handleResourceReceiveProgress(from: from, name: name, fraction: fraction, completedBytes: completedBytes, totalBytes: totalBytes)

        case .resourceReceiveFinished(let from, let name, let stagedURL, let error):
            await handleResourceReceiveFinished(from: from, name: name, stagedURL: stagedURL, error: error)
        }
    }

    // MARK: Connection lifecycle

    private func handlePeerStateChange(_ box: MCPeerIDBox, _ state: PeerConnectionState) {
        switch state {
        case .connecting:
            break

        case .connected:
            let stableID = connected[box]?.stableID ?? discovered[box]?.stableID
            let peer = Peer(box: box, stableID: stableID)
            connected[box] = peer
            resumeConnectContinuations(for: box, with: .success(()))
            sendHello(to: box)
            yieldEvent(.peerConnected(peer))

        case .notConnected:
            if let peer = connected.removeValue(forKey: box) {
                yieldEvent(.peerDisconnected(peer))
                handlePeerDropped(box, peer: peer)
            } else {
                let peer = discovered[box] ?? Peer(box: box)
                resumeConnectContinuations(for: box, with: .failure(LocalSyncError.connectionFailed(peer)))
            }
        }
    }

    /// Take-then-resume: continuations are removed from the registry before
    /// resuming, so a timeout, a cancellation, and this state change can race
    /// without a double resume.
    private func resumeConnectContinuations(for box: MCPeerIDBox, with result: Result<Void, any Error>) {
        guard var pending = pendingConnects.removeValue(forKey: box) else { return }
        let continuations = pending.continuations
        pending.continuations = [:]
        for watchdog in pending.watchdogs.values {
            watchdog.cancel()
        }
        for continuation in continuations.values {
            continuation.resume(with: result)
        }
    }

    /// Removes and resumes exactly one attempt (used by per-attempt timeout
    /// and cancellation so unrelated attempts to the same peer survive).
    private func takeConnectAttempt(_ box: MCPeerIDBox, attempt: UUID) -> CheckedContinuation<Void, any Error>? {
        guard var pending = pendingConnects[box] else { return nil }
        let continuation = pending.continuations.removeValue(forKey: attempt)
        pending.watchdogs.removeValue(forKey: attempt)?.cancel()
        if pending.continuations.isEmpty {
            pendingConnects.removeValue(forKey: box)
        } else {
            pendingConnects[box] = pending
        }
        return continuation
    }

    func connect(to peer: Peer) async throws {
        try LocalSync.validateServiceName(serviceName)
        if connected[peer.box] != nil { return }
        try startBrowsingIfNeeded()

        let attemptID = UUID()
        let box = peer.box
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                // Runs synchronously inside the actor, so registration cannot
                // race with event handling. If the task was cancelled before
                // we got here, the cancellation handler has already run and
                // found nothing — resume immediately.
                if Task.isCancelled {
                    continuation.resume(throwing: CancellationError())
                    return
                }
                var pending = pendingConnects[box] ?? PendingConnect()
                pending.continuations[attemptID] = continuation
                pending.watchdogs[attemptID] = Task { [weak self, timeout = configuration.connectTimeout] in
                    try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                    guard !Task.isCancelled else { return }
                    await self?.timeoutConnect(box, attempt: attemptID, peer: peer)
                }
                pendingConnects[box] = pending

                let context = try? JSONEncoder().encode(localHello())
                transport.invite(box, context: context, timeout: configuration.connectTimeout)
            }
        } onCancel: {
            Task { [engine = self] in
                await engine.cancelConnect(box, attempt: attemptID)
            }
        }
    }

    private func timeoutConnect(_ box: MCPeerIDBox, attempt: UUID, peer: Peer) {
        takeConnectAttempt(box, attempt: attempt)?
            .resume(throwing: LocalSyncError.connectionTimedOut(peer))
    }

    private func cancelConnect(_ box: MCPeerIDBox, attempt: UUID) {
        takeConnectAttempt(box, attempt: attempt)?
            .resume(throwing: CancellationError())
    }

    // MARK: Invitations (receiving side)

    private func handleInvitation(_ box: MCPeerIDBox, context: Data?, responder: InvitationResponder) {
        var stableID: String?
        if let context, let hello = try? JSONDecoder().decode(HelloPayload.self, from: context) {
            stableID = hello.stableID
        }
        let peer = Peer(box: box, stableID: stableID)
        let id = UUID()
        pendingInvitations[id] = PendingInvitation(peer: peer, responder: responder)
        let request = ConnectionRequest(id: id, from: peer, engine: self)
        for continuation in requestSubscribers.values {
            continuation.yield(request)
        }
    }

    func respondToInvitation(id: UUID, accept: Bool) async throws {
        guard var invitation = pendingInvitations[id], !invitation.responded else {
            throw LocalSyncError.alreadyResponded
        }
        invitation.responded = true
        pendingInvitations[id] = invitation
        let peer = invitation.peer
        let box = peer.box

        guard accept else {
            invitation.responder.respond(false)
            pendingInvitations.removeValue(forKey: id)
            return
        }

        if connected[box] != nil {
            invitation.responder.respond(true)
            pendingInvitations.removeValue(forKey: id)
            return
        }

        // Wait for the session to report the peer as connected, through the
        // same keyed-continuation path `connect(to:)` uses.
        let attemptID = UUID()
        defer { pendingInvitations.removeValue(forKey: id) }
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                if Task.isCancelled {
                    continuation.resume(throwing: CancellationError())
                    return
                }
                var pending = pendingConnects[box] ?? PendingConnect()
                pending.continuations[attemptID] = continuation
                pending.watchdogs[attemptID] = Task { [weak self, timeout = configuration.connectTimeout] in
                    try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                    guard !Task.isCancelled else { return }
                    await self?.timeoutConnect(box, attempt: attemptID, peer: peer)
                }
                pendingConnects[box] = pending
                invitation.responder.respond(true)
            }
        } onCancel: {
            Task { [engine = self] in
                await engine.cancelConnect(box, attempt: attemptID)
            }
        }
    }

    // MARK: Hello exchange

    func localHello() -> HelloPayload {
        HelloPayload(
            stableID: localStableID,
            displayName: localDisplayName,
            protocolVersion: LocalSync.protocolVersion
        )
    }

    private func sendHello(to box: MCPeerIDBox) {
        guard let data = try? JSONEncoder().encode(ControlMessage.hello(localHello())) else { return }
        try? transport.send(data, to: [box])
    }

    func applyHello(_ hello: HelloPayload, from box: MCPeerIDBox) {
        if var peer = connected[box] {
            peer.stableID = hello.stableID
            connected[box] = peer
        }
        if var peer = discovered[box] {
            peer.stableID = hello.stableID
            discovered[box] = peer
        }
    }

    // MARK: Discovery control

    func startBrowsing() throws {
        try LocalSync.validateServiceName(serviceName)
        try startBrowsingIfNeeded()
    }

    private func startBrowsingIfNeeded() throws {
        guard !browsingStarted else { return }
        try transport.startBrowsing()
        browsingStarted = true
    }

    func stopBrowsing() {
        transport.stopBrowsing()
        browsingStarted = false
        discovered.removeAll()
    }

    func startAdvertising() throws {
        try LocalSync.validateServiceName(serviceName)
        guard !advertisingStarted else { return }
        try transport.startAdvertising(discoveryInfo: [
            DiscoveryInfoKey.stableID: localStableID,
            DiscoveryInfoKey.protocolVersion: String(LocalSync.protocolVersion),
        ])
        advertisingStarted = true
    }

    func stopAdvertising() {
        transport.stopAdvertising()
        advertisingStarted = false
    }

    func shutdown() {
        transport.disconnect()
        browsingStarted = false
        advertisingStarted = false
        for box in Array(pendingConnects.keys) {
            let peer = connected[box] ?? discovered[box] ?? Peer(box: box)
            resumeConnectContinuations(for: box, with: .failure(LocalSyncError.connectionFailed(peer)))
        }
        for (box, peer) in connected {
            yieldEvent(.peerDisconnected(peer))
            handlePeerDropped(box, peer: peer)
        }
        connected.removeAll()
        discovered.removeAll()
    }

    // MARK: Queries

    func currentConnectedPeers() -> [Peer] {
        Array(connected.values)
    }

    func attachSyncState(_ store: SyncStateStore) {
        syncStore = store
    }

    // MARK: Streams

    func nearbyPeersStream() -> AsyncStream<Peer> {
        try? startBrowsingIfNeeded()
        let id = UUID()
        let (stream, continuation) = AsyncStream.makeStream(of: Peer.self)
        for peer in discovered.values {
            continuation.yield(peer)
        }
        peerSubscribers[id] = continuation
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removePeerSubscriber(id) }
        }
        return stream
    }

    func connectionRequestsStream() -> AsyncStream<ConnectionRequest> {
        try? startAdvertising()
        let id = UUID()
        let (stream, continuation) = AsyncStream.makeStream(of: ConnectionRequest.self)
        for (invitationID, invitation) in pendingInvitations where !invitation.responded {
            continuation.yield(ConnectionRequest(id: invitationID, from: invitation.peer, engine: self))
        }
        requestSubscribers[id] = continuation
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removeRequestSubscriber(id) }
        }
        return stream
    }

    func eventsStream() -> AsyncStream<SyncEvent> {
        let id = UUID()
        let (stream, continuation) = AsyncStream.makeStream(of: SyncEvent.self)
        eventSubscribers[id] = continuation
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removeEventSubscriber(id) }
        }
        return stream
    }

    func yieldEvent(_ event: SyncEvent) {
        for continuation in eventSubscribers.values {
            continuation.yield(event)
        }
    }

    private func removePeerSubscriber(_ id: UUID) { peerSubscribers.removeValue(forKey: id) }
    private func removeEventSubscriber(_ id: UUID) { eventSubscribers.removeValue(forKey: id) }
    private func removeRequestSubscriber(_ id: UUID) { requestSubscribers.removeValue(forKey: id) }
    func removeIncomingSubscriber(_ id: UUID) { incomingSubscribers.removeValue(forKey: id) }
}

/// Keys used in the Bonjour TXT record advertised during discovery.
enum DiscoveryInfoKey {
    static let stableID = "lfsid"
    static let protocolVersion = "lfsv"
}
