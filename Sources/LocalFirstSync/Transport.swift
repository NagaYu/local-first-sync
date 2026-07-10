import Foundation
import MultipeerConnectivity
import os

// MARK: - Transport events

/// The connection state of a peer, mirrored from `MCSessionState` into a
/// Sendable value.
enum PeerConnectionState: Sendable {
    case connecting
    case connected
    case notConnected
}

/// Everything the underlying transport can tell the engine, as Sendable
/// values. Every delegate callback becomes exactly one event, yielded
/// synchronously from the callback so that arrival order on Multipeer
/// Connectivity's private serial queue is preserved end-to-end. The engine
/// consumes these with a single `for await` loop, which serializes all state
/// mutation — the "delegate → AsyncStream mailbox → actor" pattern.
enum TransportEvent: Sendable {
    // Discovery
    case peerFound(MCPeerIDBox, discoveryInfo: [String: String]?)
    case peerLost(MCPeerIDBox)
    // Connection lifecycle
    case peerStateChanged(MCPeerIDBox, PeerConnectionState)
    case invitationReceived(MCPeerIDBox, context: Data?, respond: InvitationResponder)
    // Control-plane data
    case dataReceived(from: MCPeerIDBox, Data)
    // Outgoing resource lifecycle (correlated by token)
    case resourceSendProgressed(token: UUID, fraction: Double, completedBytes: Int64, totalBytes: Int64)
    case resourceSendFinished(token: UUID, error: String?)
    // Incoming resource lifecycle (correlated by wire name)
    case resourceReceiveStarted(from: MCPeerIDBox, name: String)
    case resourceReceiveProgressed(from: MCPeerIDBox, name: String, fraction: Double, completedBytes: Int64, totalBytes: Int64)
    /// `stagedURL` points at a file this package already moved out of the
    /// framework's temporary location (which is deleted the moment the
    /// delegate callback returns) into its own staging directory.
    case resourceReceiveFinished(from: MCPeerIDBox, name: String, stagedURL: URL?, error: String?)
}

/// Carries a value the SDK hasn't audited for Sendable across an isolation
/// boundary. Use only for values that are immutable or externally serialized.
struct UncheckedSendableBox<Value>: @unchecked Sendable {
    let value: Value
}

/// Calls the advertiser's invitation handler exactly once, no matter how many
/// times or from how many tasks `respond` is invoked.
final class InvitationResponder: Sendable {
    private let handler: @Sendable (Bool) -> Void
    private let responded = OSAllocatedUnfairLock(initialState: false)

    init(_ handler: @escaping @Sendable (Bool) -> Void) {
        self.handler = handler
    }

    /// Returns false if a response was already delivered.
    @discardableResult
    func respond(_ accept: Bool) -> Bool {
        let first = responded.withLock { alreadyResponded -> Bool in
            if alreadyResponded { return false }
            alreadyResponded = true
            return true
        }
        if first { handler(accept) }
        return first
    }
}

// MARK: - Transport protocol

/// Seam between the engine and Multipeer Connectivity.
///
/// Exists for two reasons: it lets unit tests drive the engine with scripted
/// event interleavings and inspect outgoing calls without radios or TCC
/// prompts, and it isolates the one framework Apple has deprecated as of the
/// OS 27 betas so a Network-framework transport can slot in later.
protocol MultipeerTransport: Sendable {
    /// The single, ordered event mailbox. Consumed by exactly one loop.
    var events: AsyncStream<TransportEvent> { get }

    func startBrowsing() throws
    func stopBrowsing()
    func startAdvertising(discoveryInfo: [String: String]) throws
    func stopAdvertising()

    /// Invites a discovered peer into this device's session.
    func invite(_ peer: MCPeerIDBox, context: Data?, timeout: TimeInterval)

    /// Sends a small control message reliably (in order) to the given peers.
    func send(_ data: Data, to peers: [MCPeerIDBox]) throws

    /// Starts one resource transfer to one peer. Progress and completion are
    /// reported on `events`, correlated by `token`.
    func sendResource(at url: URL, name: String, to peer: MCPeerIDBox, token: UUID)

    /// Cancels an in-flight outgoing resource transfer.
    func cancelResourceSend(token: UUID)

    /// Tears down the session and stops all discovery.
    func disconnect()
}

// MARK: - MCTransport

/// The production transport, wrapping one `MCSession` (capable of holding up
/// to 8 simultaneously connected peers, per the framework), one browser, and
/// one advertiser.
///
/// Thread-safety: `MCSession`, `MCNearbyServiceBrowser`, and
/// `MCNearbyServiceAdvertiser` are not Sendable and are confined here. All
/// mutable state is behind `OSAllocatedUnfairLock`. Methods are called from
/// the engine actor; delegate callbacks arrive on the framework's private
/// serial queue and only yield events or perform the synchronous file move
/// that `didFinishReceivingResource` requires.
final class MCTransport: NSObject, MultipeerTransport, @unchecked Sendable {
    let events: AsyncStream<TransportEvent>
    private let continuation: AsyncStream<TransportEvent>.Continuation

    private let localPeerID: MCPeerID
    private let serviceType: String
    private let session: MCSession
    private let stagingDirectory: URL

    private struct MutableState {
        var browser: MCNearbyServiceBrowser?
        var advertiser: MCNearbyServiceAdvertiser?
        var outgoingResources: [UUID: OutgoingResource] = [:]
        var incomingObservations: [String: NSKeyValueObservation] = [:]
    }

    private struct OutgoingResource {
        let progress: Progress?
        let observation: NSKeyValueObservation?
    }

    private let state: OSAllocatedUnfairLock<MutableState>

    init(displayName: String, serviceType: String) {
        self.localPeerID = MCPeerID(displayName: displayName)
        self.serviceType = serviceType
        // Hard requirement of this package: peers exchange a user's own
        // photos and files, so the session demands encryption — never
        // `.none`, never `.optional`.
        self.session = MCSession(
            peer: localPeerID,
            securityIdentity: nil,
            encryptionPreference: .required
        )
        (self.events, self.continuation) = AsyncStream.makeStream(
            of: TransportEvent.self,
            bufferingPolicy: .unbounded
        )
        self.stagingDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("LocalFirstSync-staging-\(UUID().uuidString)", isDirectory: true)
        self.state = OSAllocatedUnfairLock(uncheckedState: MutableState())
        super.init()
        session.delegate = self
    }

    deinit {
        continuation.finish()
    }

    // MARK: Discovery

    func startBrowsing() throws {
        state.withLockUnchecked { s in
            guard s.browser == nil else { return }
            let browser = MCNearbyServiceBrowser(peer: localPeerID, serviceType: serviceType)
            browser.delegate = self
            browser.startBrowsingForPeers()
            s.browser = browser
        }
    }

    func stopBrowsing() {
        state.withLockUnchecked { s in
            s.browser?.stopBrowsingForPeers()
            s.browser?.delegate = nil
            s.browser = nil
        }
    }

    func startAdvertising(discoveryInfo: [String: String]) throws {
        state.withLockUnchecked { s in
            guard s.advertiser == nil else { return }
            let advertiser = MCNearbyServiceAdvertiser(
                peer: localPeerID,
                discoveryInfo: discoveryInfo,
                serviceType: serviceType
            )
            advertiser.delegate = self
            advertiser.startAdvertisingPeer()
            s.advertiser = advertiser
        }
    }

    func stopAdvertising() {
        state.withLockUnchecked { s in
            s.advertiser?.stopAdvertisingPeer()
            s.advertiser?.delegate = nil
            s.advertiser = nil
        }
    }

    // MARK: Session operations

    func invite(_ peer: MCPeerIDBox, context: Data?, timeout: TimeInterval) {
        let browser = state.withLockUnchecked { $0.browser }
        browser?.invitePeer(peer.peerID, to: session, withContext: context, timeout: timeout)
    }

    func send(_ data: Data, to peers: [MCPeerIDBox]) throws {
        guard !peers.isEmpty else { return }
        // .reliable is the only mode with a documented in-order delivery
        // guarantee; the control protocol depends on it.
        try session.send(data, toPeers: peers.map(\.peerID), with: .reliable)
    }

    func sendResource(at url: URL, name: String, to peer: MCPeerIDBox, token: UUID) {
        let continuation = self.continuation
        let progress = session.sendResource(at: url, withName: name, toPeer: peer.peerID) { error in
            continuation.yield(.resourceSendFinished(token: token, error: error?.localizedDescription))
        }
        var observation: NSKeyValueObservation?
        if let progress {
            let throttle = ProgressThrottle()
            observation = progress.observe(\.fractionCompleted, options: [.initial, .new]) { progress, _ in
                guard throttle.shouldReport(progress.fractionCompleted) else { return }
                continuation.yield(.resourceSendProgressed(
                    token: token,
                    fraction: progress.fractionCompleted,
                    completedBytes: progress.completedUnitCount,
                    totalBytes: progress.totalUnitCount
                ))
            }
        }
        state.withLockUnchecked { s in
            s.outgoingResources[token] = OutgoingResource(progress: progress, observation: observation)
        }
    }

    func cancelResourceSend(token: UUID) {
        let resource = state.withLockUnchecked { $0.outgoingResources.removeValue(forKey: token) }
        resource?.observation?.invalidate()
        resource?.progress?.cancel()
    }

    func disconnect() {
        stopBrowsing()
        stopAdvertising()
        session.disconnect()
        let observations = state.withLockUnchecked { s in
            let all = s.outgoingResources
            s.outgoingResources = [:]
            s.incomingObservations = [:]
            return all
        }
        for resource in observations.values {
            resource.observation?.invalidate()
        }
    }

    private func releaseOutgoing(token: UUID) {
        let resource = state.withLockUnchecked { $0.outgoingResources.removeValue(forKey: token) }
        resource?.observation?.invalidate()
    }
}

/// Coalesces KVO progress callbacks so the event mailbox is not flooded:
/// reports when the fraction advances by at least 1% or reaches completion.
private final class ProgressThrottle: @unchecked Sendable {
    private let lock = OSAllocatedUnfairLock(initialState: -1.0)

    func shouldReport(_ fraction: Double) -> Bool {
        lock.withLock { last in
            if fraction >= 1.0 || fraction - last >= 0.01 || fraction < last {
                last = fraction
                return true
            }
            return false
        }
    }
}

// MARK: - MCSessionDelegate

/// The delegate conformances are deliberately nonisolated: Multipeer
/// Connectivity calls them on its private serial queue
/// (`com.apple.MCSession.syncQueue`), and a globally-isolated conformance
/// would trap at runtime under Swift 6. Every method only converts its
/// arguments to a Sendable event and yields it — the yield is synchronous, so
/// the queue's ordering carries through to the engine's consumption order.
extension MCTransport: MCSessionDelegate {
    func session(_ session: MCSession, peer peerID: MCPeerID, didChange state: MCSessionState) {
        let mapped: PeerConnectionState = switch state {
        case .connecting: .connecting
        case .connected: .connected
        case .notConnected: .notConnected
        @unknown default: .notConnected
        }
        continuation.yield(.peerStateChanged(MCPeerIDBox(peerID), mapped))
    }

    func session(_ session: MCSession, didReceive data: Data, fromPeer peerID: MCPeerID) {
        continuation.yield(.dataReceived(from: MCPeerIDBox(peerID), data))
    }

    func session(
        _ session: MCSession,
        didStartReceivingResourceWithName resourceName: String,
        fromPeer peerID: MCPeerID,
        with progress: Progress
    ) {
        let box = MCPeerIDBox(peerID)
        continuation.yield(.resourceReceiveStarted(from: box, name: resourceName))
        let continuation = self.continuation
        let throttle = ProgressThrottle()
        let observation = progress.observe(\.fractionCompleted, options: [.initial, .new]) { progress, _ in
            guard throttle.shouldReport(progress.fractionCompleted) else { return }
            continuation.yield(.resourceReceiveProgressed(
                from: box,
                name: resourceName,
                fraction: progress.fractionCompleted,
                completedBytes: progress.completedUnitCount,
                totalBytes: progress.totalUnitCount
            ))
        }
        let key = Self.incomingKey(peerID: peerID, name: resourceName)
        state.withLockUnchecked { s in
            s.incomingObservations[key] = observation
        }
    }

    func session(
        _ session: MCSession,
        didFinishReceivingResourceWithName resourceName: String,
        fromPeer peerID: MCPeerID,
        at localURL: URL?,
        withError error: (any Error)?
    ) {
        let key = Self.incomingKey(peerID: peerID, name: resourceName)
        let observation = state.withLockUnchecked { $0.incomingObservations.removeValue(forKey: key) }
        observation?.invalidate()

        // The framework deletes `localURL` the moment this method returns, so
        // the move to our own staging directory must happen synchronously
        // here — not after a hop to the engine actor.
        var stagedURL: URL?
        var errorMessage = error?.localizedDescription
        if let localURL, error == nil {
            do {
                let fm = FileManager.default
                try fm.createDirectory(at: stagingDirectory, withIntermediateDirectories: true)
                let destination = stagingDirectory
                    .appendingPathComponent(UUID().uuidString, isDirectory: false)
                    .appendingPathExtension(localURL.pathExtension)
                try fm.moveItem(at: localURL, to: destination)
                stagedURL = destination
            } catch {
                errorMessage = "Could not stage received file: \(error.localizedDescription)"
            }
        }
        continuation.yield(.resourceReceiveFinished(
            from: MCPeerIDBox(peerID),
            name: resourceName,
            stagedURL: stagedURL,
            error: errorMessage
        ))
    }

    func session(
        _ session: MCSession,
        didReceive stream: InputStream,
        withName streamName: String,
        fromPeer peerID: MCPeerID
    ) {
        // This package does not use the byte-stream API.
        stream.close()
    }

    private static func incomingKey(peerID: MCPeerID, name: String) -> String {
        "\(UInt(bitPattern: peerID.hash))|\(name)"
    }
}

// MARK: - Browser & advertiser delegates

extension MCTransport: MCNearbyServiceBrowserDelegate {
    func browser(_ browser: MCNearbyServiceBrowser, foundPeer peerID: MCPeerID, withDiscoveryInfo info: [String: String]?) {
        continuation.yield(.peerFound(MCPeerIDBox(peerID), discoveryInfo: info))
    }

    func browser(_ browser: MCNearbyServiceBrowser, lostPeer peerID: MCPeerID) {
        continuation.yield(.peerLost(MCPeerIDBox(peerID)))
    }
}

extension MCTransport: MCNearbyServiceAdvertiserDelegate {
    func advertiser(
        _ advertiser: MCNearbyServiceAdvertiser,
        didReceiveInvitationFromPeer peerID: MCPeerID,
        withContext context: Data?,
        invitationHandler: @escaping (Bool, MCSession?) -> Void
    ) {
        // No silent trust: the invitation is surfaced to the app and nothing
        // is accepted automatically. The responder guarantees the handler is
        // called at most once even if accept/decline race.
        //
        // Neither the handler nor MCSession is audited for Sendable by the
        // SDK; both are immutable references invoked from at most one call
        // site (the responder's call-once gate), so boxing them is sound.
        let handlerBox = UncheckedSendableBox(value: invitationHandler)
        let sessionBox = UncheckedSendableBox(value: session)
        let responder = InvitationResponder { accept in
            handlerBox.value(accept, accept ? sessionBox.value : nil)
        }
        continuation.yield(.invitationReceived(MCPeerIDBox(peerID), context: context, respond: responder))
    }
}
