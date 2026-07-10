import Foundation
import MultipeerConnectivity
import Photos
import os

// MARK: - Control protocol

/// Small, reliable, in-order messages exchanged over the session's data
/// channel. The consent handshake lives here: no resource bytes ever flow to
/// a peer that hasn't answered an ``offer`` with ``accept``.
enum ControlMessage: Codable, Sendable {
    /// First message after connecting: identity and protocol version.
    case hello(HelloPayload)
    /// "I'd like to send you this." Describes the transfer; sends no bytes.
    case offer(OfferPayload)
    /// Receiver consents; the sender may now start `sendResource`.
    case accept(transferID: UUID)
    /// Receiver refuses; the sender never starts the resource transfer.
    case decline(transferID: UUID)
    /// Sender withdrew the offer or aborted the transfer.
    case cancel(transferID: UUID)
    /// Receiver confirms every file arrived and was moved into place. Only
    /// now does the sender's sync state record this peer as synced.
    case received(transferID: UUID)
}

/// Everything the receiving side needs to show a meaningful accept/decline
/// prompt before any data flows.
struct OfferPayload: Codable, Sendable {
    var transferID: UUID
    var contentID: String
    var kind: TransferContentKind
    var items: [TransferItemMetadata]
    var totalByteCount: Int64?
}

/// Encodes transfer identity into `sendResource` names so the receiver can
/// correlate arriving resources with accepted offers — and discard anything
/// unsolicited.
enum WireFormat {
    static let prefix = "lfs1"

    static func resourceName(transferID: UUID, itemID: UUID, filename: String) -> String {
        "\(prefix)|\(transferID.uuidString)|\(itemID.uuidString)|\(filename)"
    }

    static func parseResourceName(_ name: String) -> (transferID: UUID, itemID: UUID, filename: String)? {
        let parts = name.split(separator: "|", maxSplits: 3, omittingEmptySubsequences: false)
        guard parts.count == 4,
              parts[0] == Substring(prefix),
              let transferID = UUID(uuidString: String(parts[1])),
              let itemID = UUID(uuidString: String(parts[2]))
        else { return nil }
        return (transferID, itemID, String(parts[3]))
    }
}

// MARK: - Public sending API

extension LocalSync {
    /// Sends a photo-library asset (photo, Live Photo, or video) to one or
    /// more connected peers, preserving the original format — no re-encoding.
    ///
    /// The returned ``Transfer`` is an `AsyncSequence` of per-peer
    /// ``TransferUpdate`` values: broadcasting to three peers produces three
    /// independent progress series, never one blended average.
    ///
    /// If the asset's original bytes live in iCloud (the *user's own* iCloud
    /// Photos setting — unrelated to this package's no-cloud transfer, which
    /// stays on the local network), they are downloaded first;
    /// ``Transfer/preparationProgress()`` reports that phase.
    ///
    /// No bytes are sent to any peer until that peer explicitly accepts the
    /// offer on their device.
    public func send(photo asset: PHAsset, to target: TransferTarget) -> Transfer {
        // Capture only the identifier: PHAsset is not Sendable, and the
        // export re-fetches it inside the exporter.
        startSend(
            source: .photoAsset(localIdentifier: asset.localIdentifier),
            contentID: asset.localIdentifier,
            target: target
        )
    }

    /// Sends an arbitrary file to one or more connected peers.
    ///
    /// Same consent and per-peer progress semantics as ``send(photo:to:)``.
    public func send(contentsOf url: URL, to target: TransferTarget) -> Transfer {
        startSend(source: .file(url), contentID: url.absoluteString, target: target)
    }

    private func startSend(source: TransferSource, contentID: String, target: TransferTarget) -> Transfer {
        let id = UUID()
        let engine = self.engine
        Task {
            await engine.startOutgoingTransfer(id: id, source: source, contentID: contentID, target: target)
        }
        return Transfer(id: id, engine: engine)
    }
}

enum TransferSource: Sendable {
    case photoAsset(localIdentifier: String)
    case file(URL)
}

// MARK: - Transfer (outgoing handle)

/// A handle to one outgoing transfer. Iterate it for per-peer updates, or
/// `try await transfer.waitUntilCompleted()` for a summary.
public struct Transfer: Sendable, AsyncSequence {
    public typealias Element = TransferUpdate

    /// Identifies this transfer across updates and sync-state records.
    public let id: UUID
    let engine: SyncEngine

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(id: id, engine: engine)
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        let id: UUID
        let engine: SyncEngine
        var inner: AsyncStream<TransferUpdate>.AsyncIterator?

        public mutating func next() async -> TransferUpdate? {
            if inner == nil {
                inner = await engine.transferUpdatesStream(id: id).makeAsyncIterator()
            }
            return await inner?.next()
        }
    }

    /// Per-peer updates. Each call returns an independent stream; the latest
    /// state of every peer is replayed first, then live updates follow.
    public func updates() async -> AsyncStream<TransferUpdate> {
        await engine.transferUpdatesStream(id: id)
    }

    /// Progress of the local preparation phase (photo export, including any
    /// iCloud original download), from 0.0 to 1.0. Finishes when sending
    /// begins. Empty for plain-file transfers.
    public func preparationProgress() async -> AsyncStream<Double> {
        await engine.preparationProgressStream(id: id)
    }

    /// Waits until every targeted peer reaches a terminal state.
    ///
    /// - Returns: The per-peer outcome when at least one peer completed.
    /// - Throws: The transfer-level error if preparation failed or no peers
    ///   were available; otherwise, if *no* peer completed, the first peer's
    ///   error (or ``LocalSyncError/cancelled`` when everyone declined).
    @discardableResult
    public func waitUntilCompleted() async throws -> TransferSummary {
        let summary = try await engine.awaitSummary(id: id)
        if summary.anySucceeded || (summary.failed.isEmpty && summary.declined.isEmpty) {
            return summary
        }
        if let firstFailure = summary.failed.first {
            throw firstFailure.error
        }
        throw LocalSyncError.cancelled
    }

    /// Cancels the transfer for every peer that hasn't finished. Peers that
    /// already completed stay completed.
    public func cancel() async {
        await engine.cancelOutgoingTransfer(id: id)
    }
}

// MARK: - Incoming transfers (receiving side)

extension LocalSync {
    /// Offers from connected peers, each awaiting an explicit
    /// ``IncomingTransfer/accept(into:)`` or ``IncomingTransfer/decline()``.
    ///
    /// No file data flows for an offer until it is accepted — discovery and
    /// even connection are never enough for a peer to push content here.
    public func incomingTransfers() -> AsyncStream<IncomingTransfer> {
        let engine = self.engine
        let (stream, continuation) = AsyncStream.makeStream(of: IncomingTransfer.self)
        let task = Task {
            let inner = await engine.incomingTransfersStream()
            for await incoming in inner {
                continuation.yield(incoming)
            }
            continuation.finish()
        }
        continuation.onTermination = { _ in task.cancel() }
        return stream
    }
}

/// An offer from a connected peer. Inspect ``items`` and ``totalByteCount``,
/// then ``accept(into:)`` or ``decline()``. Until accepted, the sender
/// transmits nothing but this metadata.
public struct IncomingTransfer: Sendable, Identifiable {
    /// The transfer's identity (shared with the sender's ``Transfer/id``).
    public let id: UUID
    /// The peer offering the content.
    public let from: Peer
    /// What kind of content is being offered.
    public let kind: TransferContentKind
    /// The files that would be transferred.
    public let items: [TransferItemMetadata]
    /// Total size in bytes, when the sender knew it.
    public let totalByteCount: Int64?

    let engine: SyncEngine

    /// Accepts the offer. File data starts flowing from the sender.
    ///
    /// - Parameter directory: Where received files are stored. Defaults to a
    ///   per-transfer folder under the caches directory; move files somewhere
    ///   durable (or into the photo library via
    ///   ``PhotoLibraryImporter``) when the transfer completes.
    /// - Throws: ``LocalSyncError/alreadyResponded`` or
    ///   ``LocalSyncError/cancelled`` if the sender withdrew the offer.
    public func accept(into directory: URL? = nil) async throws -> IncomingTransferSession {
        try await engine.acceptOffer(id: id, into: directory)
    }

    /// Declines the offer. The sender sees this peer as declined; other
    /// peers in the same broadcast are unaffected.
    public func decline() async {
        await engine.declineOffer(id: id)
    }
}

/// Live view of an accepted incoming transfer.
public struct IncomingTransferSession: Sendable {
    /// The transfer's identity.
    public let id: UUID
    let engine: SyncEngine

    /// Aggregate progress of the incoming files, 0.0 to 1.0.
    public func progress() async -> AsyncStream<TransferProgress> {
        await engine.offerProgressStream(id: id)
    }

    /// Waits for every file to arrive and be moved into the destination
    /// directory, then returns them in offer order.
    public func files() async throws -> [ReceivedFile] {
        try await engine.awaitReceivedFiles(id: id)
    }
}

/// One file received from a peer, moved into its destination directory.
public struct ReceivedFile: Sendable, Hashable {
    /// Where the file now lives.
    public let url: URL
    /// The sender-provided description of the file.
    public let metadata: TransferItemMetadata
}

// MARK: - Engine: outgoing transfer state machine

/// Per-peer lifecycle of an outgoing transfer. Each peer advances through
/// this machine independently; one peer failing, declining, or disconnecting
/// never touches another peer's state.
enum OutgoingPeerPhase {
    /// Export still running; offer not yet sent.
    case pending
    /// Offer sent; waiting for the peer's user.
    case offered
    /// Peer accepted; resource bytes are flowing.
    case sending
    /// All bytes delivered; waiting for the receiver's storage confirmation.
    case awaitingConfirmation
    case completed
    case declined
    case failed(LocalSyncError)

    var isTerminal: Bool {
        switch self {
        case .completed, .declined, .failed: true
        default: false
        }
    }
}

struct OutgoingPeerState {
    let peer: Peer
    var phase: OutgoingPeerPhase = .pending
    var currentItemIndex = 0
    var deliveredBytes: Int64 = 0
    var currentItemCompletedBytes: Int64 = 0
    var currentItemFraction: Double = 0
    var currentToken: UUID?
    var ackReceived = false
    var watchdog: Task<Void, Never>?
}

struct OutgoingTransferState {
    let id: UUID
    var contentID: String = ""
    var kind: TransferContentKind = .file
    var items: [PreparedItem] = []
    var totalBytes: Int64 = 0
    var prepared = false
    var transferError: LocalSyncError?
    var started = false
    var peers: [MCPeerIDBox: OutgoingPeerState] = [:]
    var lastUpdates: [MCPeerIDBox: TransferUpdate] = [:]
    var updateSubscribers: [UUID: AsyncStream<TransferUpdate>.Continuation] = [:]
    var preparationSubscribers: [UUID: AsyncStream<Double>.Continuation] = [:]
    var preparationFraction: Double = 0
    var summaryWaiters: [CheckedContinuation<TransferSummary, any Error>] = []
    var exportTask: Task<Void, Never>?
    var temporaryDirectory: URL?
    var finished = false
}

/// A file staged for sending, with the metadata the receiver needs.
struct PreparedItem: Sendable, Hashable {
    let metadata: TransferItemMetadata
    let fileURL: URL
}

extension SyncEngine {
    // MARK: Starting

    func startOutgoingTransfer(id: UUID, source: TransferSource, contentID: String, target: TransferTarget) async {
        var state = outgoingTransfers[id] ?? OutgoingTransferState(id: id)
        guard !state.started else { return }
        state.started = true
        state.contentID = contentID

        // cancel() can land before this start task runs; honor it.
        if state.transferError != nil {
            state.prepared = true
            outgoingTransfers[id] = state
            finishTransferIfDone(id: id)
            return
        }

        // Resolve targets now, at the moment the developer called send.
        let targets: [Peer]
        switch target.kind {
        case .allConnectedPeers:
            targets = Array(connected.values)
        case .peers(let requested):
            targets = requested
        }

        guard !targets.isEmpty else {
            state.transferError = .noPeersConnected
            outgoingTransfers[id] = state
            finishTransferIfDone(id: id)
            return
        }

        for peer in targets {
            var peerState = OutgoingPeerState(peer: peer)
            if connected[peer.box] == nil {
                peerState.phase = .failed(.peerNotConnected(peer))
            }
            state.peers[peer.box] = peerState
        }
        outgoingTransfers[id] = state

        // Surface the immediate failures (disconnected explicit targets).
        for (box, peerState) in state.peers {
            if case .failed(let error) = peerState.phase {
                emitUpdate(transferID: id, box: box, phase: .failed(error), progress: .none)
                recordSyncState(contentID: contentID, peer: peerState.peer) { store, content, identity in
                    try await store.markFailed(content: content, peer: identity, reason: error.localizedDescription)
                }
            }
        }

        await prepare(id: id, source: source)
    }

    private func prepare(id: UUID, source: TransferSource) async {
        switch source {
        case .file(let url):
            var state = outgoingTransfers[id] ?? OutgoingTransferState(id: id)
            let values = try? url.resourceValues(forKeys: [.fileSizeKey, .contentTypeKey])
            let byteCount = values?.fileSize.map(Int64.init)
            let metadata = TransferItemMetadata(
                itemID: UUID(),
                filename: url.lastPathComponent,
                typeIdentifier: values?.contentType?.identifier,
                photoResourceType: nil,
                byteCount: byteCount
            )
            state.kind = .file
            state.items = [PreparedItem(metadata: metadata, fileURL: url)]
            state.totalBytes = byteCount ?? 0
            state.prepared = true
            outgoingTransfers[id] = state
            finishPreparation(id: id)
            sendOffers(id: id)

        case .photoAsset(let localIdentifier):
            let directory = FileManager.default.temporaryDirectory
                .appendingPathComponent("LocalFirstSync-export-\(id.uuidString)", isDirectory: true)
            var state = outgoingTransfers[id] ?? OutgoingTransferState(id: id)
            state.temporaryDirectory = directory
            let exporter = self.exporter
            state.exportTask = Task { [weak self] in
                do {
                    let exported = try await exporter.export(
                        assetIdentifier: localIdentifier,
                        into: directory,
                        progress: { [weak self] fraction in
                            guard let engine = self else { return }
                            Task { await engine.updatePreparationProgress(id: id, fraction: fraction) }
                        }
                    )
                    await self?.exportSucceeded(id: id, exported: exported)
                } catch {
                    let syncError = (error as? LocalSyncError) ?? .assetExportFailed(error.localizedDescription)
                    await self?.exportFailed(id: id, error: syncError)
                }
            }
            outgoingTransfers[id] = state
        }
    }

    private func updatePreparationProgress(id: UUID, fraction: Double) {
        guard var state = outgoingTransfers[id] else { return }
        // Monotonic: progress callbacks hop tasks and may arrive out of order.
        guard fraction > state.preparationFraction else { return }
        state.preparationFraction = fraction
        outgoingTransfers[id] = state
        for continuation in state.preparationSubscribers.values {
            continuation.yield(fraction)
        }
    }

    private func exportSucceeded(id: UUID, exported: ExportedAsset) {
        guard var state = outgoingTransfers[id], !state.finished else { return }
        state.kind = exported.kind
        state.items = exported.items
        state.totalBytes = exported.items.compactMap(\.metadata.byteCount).reduce(0, +)
        state.prepared = true
        outgoingTransfers[id] = state
        finishPreparation(id: id)
        sendOffers(id: id)
    }

    private func exportFailed(id: UUID, error: LocalSyncError) {
        guard var state = outgoingTransfers[id], !state.finished else { return }
        state.transferError = error
        state.prepared = true
        outgoingTransfers[id] = state
        finishPreparation(id: id)
        for (box, peerState) in state.peers where !peerState.phase.isTerminal {
            transitionPeer(transferID: id, box: box, to: .failed(error))
        }
        finishTransferIfDone(id: id)
    }

    private func finishPreparation(id: UUID) {
        guard var state = outgoingTransfers[id] else { return }
        let subscribers = state.preparationSubscribers
        state.preparationSubscribers = [:]
        outgoingTransfers[id] = state
        for continuation in subscribers.values {
            continuation.yield(1.0)
            continuation.finish()
        }
    }

    // MARK: Offers

    private func sendOffers(id: UUID) {
        guard let state = outgoingTransfers[id], state.prepared, state.transferError == nil else { return }
        let payload = OfferPayload(
            transferID: id,
            contentID: state.contentID,
            kind: state.kind,
            items: state.items.map(\.metadata),
            totalByteCount: state.totalBytes > 0 ? state.totalBytes : nil
        )
        guard let data = try? JSONEncoder().encode(ControlMessage.offer(payload)) else { return }

        for (box, peerState) in state.peers {
            guard case .pending = peerState.phase else { continue }
            guard connected[box] != nil else {
                transitionPeer(transferID: id, box: box, to: .failed(.peerNotConnected(peerState.peer)))
                continue
            }
            do {
                try transport.send(data, to: [box])
                var updated = peerState
                updated.phase = .offered
                updated.watchdog = makeWatchdog(transferID: id, box: box, timeout: configuration.acceptTimeout) { engine in
                    await engine.offerTimedOut(transferID: id, box: box)
                }
                outgoingTransfers[id]?.peers[box] = updated
                emitUpdate(transferID: id, box: box, phase: .waitingForAcceptance, progress: .none)
                recordSyncState(contentID: state.contentID, peer: peerState.peer) { store, content, identity in
                    try await store.markPending(content: content, peer: identity)
                }
            } catch {
                transitionPeer(transferID: id, box: box, to: .failed(.transportError(error.localizedDescription)))
            }
        }
        finishTransferIfDone(id: id)
    }

    private func makeWatchdog(
        transferID: UUID,
        box: MCPeerIDBox,
        timeout: TimeInterval,
        onFire: @escaping @Sendable (SyncEngine) async -> Void
    ) -> Task<Void, Never> {
        Task { [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
            guard !Task.isCancelled, let self else { return }
            await onFire(self)
        }
    }

    private func offerTimedOut(transferID: UUID, box: MCPeerIDBox) {
        guard let state = outgoingTransfers[transferID],
              let peerState = state.peers[box],
              case .offered = peerState.phase else { return }
        transitionPeer(transferID: transferID, box: box, to: .failed(.offerTimedOut))
        finishTransferIfDone(id: transferID)
    }

    // MARK: Control-message routing

    func handleControlData(from box: MCPeerIDBox, data: Data) async {
        guard let message = try? JSONDecoder().decode(ControlMessage.self, from: data) else { return }
        switch message {
        case .hello(let hello):
            applyHello(hello, from: box)
        case .offer(let payload):
            handleOffer(payload, from: box)
        case .accept(let transferID):
            handleAccept(transferID: transferID, from: box)
        case .decline(let transferID):
            handleDecline(transferID: transferID, from: box)
        case .cancel(let transferID):
            handleSenderCancelled(transferID: transferID, from: box)
        case .received(let transferID):
            handleReceivedAck(transferID: transferID, from: box)
        }
    }

    private func handleAccept(transferID: UUID, from box: MCPeerIDBox) {
        guard let state = outgoingTransfers[transferID],
              let peerState = state.peers[box],
              case .offered = peerState.phase else { return }
        peerState.watchdog?.cancel()
        var updated = peerState
        updated.phase = .sending
        updated.watchdog = nil
        outgoingTransfers[transferID]?.peers[box] = updated
        emitUpdate(transferID: transferID, box: box, phase: .sending, progress: progressSnapshot(state: state, peerState: updated))
        recordSyncState(contentID: state.contentID, peer: peerState.peer) { store, content, identity in
            try await store.markInFlight(content: content, peer: identity)
        }
        startNextItem(transferID: transferID, box: box)
    }

    private func handleDecline(transferID: UUID, from box: MCPeerIDBox) {
        guard let state = outgoingTransfers[transferID],
              let peerState = state.peers[box],
              case .offered = peerState.phase else { return }
        _ = state
        transitionPeer(transferID: transferID, box: box, to: .declined)
        finishTransferIfDone(id: transferID)
    }

    private func handleReceivedAck(transferID: UUID, from box: MCPeerIDBox) {
        guard let state = outgoingTransfers[transferID], var peerState = state.peers[box] else { return }
        switch peerState.phase {
        case .awaitingConfirmation:
            transitionPeer(transferID: transferID, box: box, to: .completed)
            finishTransferIfDone(id: transferID)
        case .sending:
            // The ack travels on the data channel and can outrun the resource
            // completion callback; remember it and complete when the last
            // completion lands.
            peerState.ackReceived = true
            outgoingTransfers[transferID]?.peers[box] = peerState
        default:
            break
        }
    }

    // MARK: Resource sending

    /// Sends items to one peer sequentially; per-peer aggregate progress
    /// stays exact because at most one item is in flight per peer.
    private func startNextItem(transferID: UUID, box: MCPeerIDBox) {
        guard let state = outgoingTransfers[transferID],
              var peerState = state.peers[box],
              case .sending = peerState.phase else { return }

        guard peerState.currentItemIndex < state.items.count else {
            allItemsDelivered(transferID: transferID, box: box)
            return
        }

        let item = state.items[peerState.currentItemIndex]
        let token = UUID()
        peerState.currentToken = token
        peerState.currentItemCompletedBytes = 0
        peerState.currentItemFraction = 0
        outgoingTransfers[transferID]?.peers[box] = peerState
        resourceTokens[token] = (transferID: transferID, box: box)
        transport.sendResource(
            at: item.fileURL,
            name: WireFormat.resourceName(
                transferID: transferID,
                itemID: item.metadata.itemID,
                filename: item.metadata.filename
            ),
            to: box,
            token: token
        )
    }

    private func allItemsDelivered(transferID: UUID, box: MCPeerIDBox) {
        guard let state = outgoingTransfers[transferID], var peerState = state.peers[box] else { return }
        if peerState.ackReceived {
            transitionPeer(transferID: transferID, box: box, to: .completed)
            finishTransferIfDone(id: transferID)
            return
        }
        peerState.phase = .awaitingConfirmation
        peerState.watchdog = makeWatchdog(transferID: transferID, box: box, timeout: configuration.confirmationTimeout) { engine in
            await engine.confirmationTimedOut(transferID: transferID, box: box)
        }
        outgoingTransfers[transferID]?.peers[box] = peerState
        emitUpdate(transferID: transferID, box: box, phase: .sending, progress: .complete)
    }

    private func confirmationTimedOut(transferID: UUID, box: MCPeerIDBox) {
        guard let state = outgoingTransfers[transferID],
              let peerState = state.peers[box],
              case .awaitingConfirmation = peerState.phase else { return }
        _ = state
        transitionPeer(transferID: transferID, box: box, to: .failed(.confirmationMissing))
        finishTransferIfDone(id: transferID)
    }

    func handleResourceSendProgress(token: UUID, fraction: Double, completedBytes: Int64, totalBytes: Int64) {
        guard let (transferID, box) = resourceTokens[token],
              let state = outgoingTransfers[transferID],
              var peerState = state.peers[box],
              peerState.currentToken == token,
              case .sending = peerState.phase else { return }
        peerState.currentItemCompletedBytes = completedBytes
        peerState.currentItemFraction = fraction
        outgoingTransfers[transferID]?.peers[box] = peerState
        emitUpdate(
            transferID: transferID,
            box: box,
            phase: .sending,
            progress: progressSnapshot(state: state, peerState: peerState)
        )
    }

    func handleResourceSendFinished(token: UUID, error: String?) async {
        guard let (transferID, box) = resourceTokens.removeValue(forKey: token),
              let state = outgoingTransfers[transferID],
              var peerState = state.peers[box],
              peerState.currentToken == token else { return }
        peerState.currentToken = nil

        if let error {
            outgoingTransfers[transferID]?.peers[box] = peerState
            if case .sending = peerState.phase {
                transitionPeer(transferID: transferID, box: box, to: .failed(.transportError(error)))
                finishTransferIfDone(id: transferID)
            }
            return
        }

        let itemBytes = state.items.indices.contains(peerState.currentItemIndex)
            ? (state.items[peerState.currentItemIndex].metadata.byteCount ?? 0)
            : 0
        peerState.deliveredBytes += itemBytes
        peerState.currentItemIndex += 1
        peerState.currentItemCompletedBytes = 0
        peerState.currentItemFraction = 0
        outgoingTransfers[transferID]?.peers[box] = peerState
        startNextItem(transferID: transferID, box: box)
    }

    private func progressSnapshot(state: OutgoingTransferState, peerState: OutgoingPeerState) -> TransferProgress {
        let completed = peerState.deliveredBytes + peerState.currentItemCompletedBytes
        if state.totalBytes > 0 {
            return TransferProgress(
                fractionCompleted: min(1.0, Double(completed) / Double(state.totalBytes)),
                completedByteCount: completed,
                totalByteCount: state.totalBytes
            )
        }
        // Sizes unknown: fall back to item-count weighting.
        let itemCount = max(1, state.items.count)
        let fraction = (Double(peerState.currentItemIndex) + peerState.currentItemFraction) / Double(itemCount)
        return TransferProgress(fractionCompleted: min(1.0, fraction), completedByteCount: nil, totalByteCount: nil)
    }

    // MARK: Peer/phase transitions

    private func transitionPeer(transferID: UUID, box: MCPeerIDBox, to phase: OutgoingPeerPhase) {
        guard let state = outgoingTransfers[transferID], var peerState = state.peers[box] else { return }
        guard !peerState.phase.isTerminal else { return }
        peerState.watchdog?.cancel()
        peerState.watchdog = nil
        if let token = peerState.currentToken {
            transport.cancelResourceSend(token: token)
            resourceTokens.removeValue(forKey: token)
            peerState.currentToken = nil
        }
        peerState.phase = phase
        outgoingTransfers[transferID]?.peers[box] = peerState

        let contentID = state.contentID
        let peer = peerState.peer
        switch phase {
        case .completed:
            emitUpdate(transferID: transferID, box: box, phase: .completed, progress: .complete)
            recordSyncState(contentID: contentID, peer: peer) { store, content, identity in
                try await store.markSynced(content: content, peer: identity)
            }
        case .declined:
            emitUpdate(transferID: transferID, box: box, phase: .declined, progress: .none)
            recordSyncState(contentID: contentID, peer: peer) { store, content, identity in
                try await store.markDeclined(content: content, peer: identity)
            }
        case .failed(let error):
            emitUpdate(transferID: transferID, box: box, phase: .failed(error), progress: .none)
            recordSyncState(contentID: contentID, peer: peer) { store, content, identity in
                try await store.markFailed(content: content, peer: identity, reason: error.localizedDescription)
            }
        case .pending, .offered, .sending, .awaitingConfirmation:
            break
        }
    }

    /// Fails this peer's in-flight pieces of every transfer — and only this
    /// peer's. Called when a connected peer drops.
    func handlePeerDropped(_ box: MCPeerIDBox, peer: Peer) {
        for (transferID, state) in outgoingTransfers {
            guard let peerState = state.peers[box], !peerState.phase.isTerminal else { continue }
            transitionPeer(transferID: transferID, box: box, to: .failed(.transportError("\(peer.displayName) disconnected")))
            finishTransferIfDone(id: transferID)
        }
        for (offerID, offer) in incomingOffers where offer.from.box == box && !offer.finished {
            failIncomingOffer(id: offerID, error: .transportError("\(peer.displayName) disconnected"))
        }
    }

    // MARK: Updates & completion

    private func emitUpdate(transferID: UUID, box: MCPeerIDBox, phase: TransferPhase, progress: TransferProgress) {
        guard var state = outgoingTransfers[transferID], let peerState = state.peers[box] else { return }
        let update = TransferUpdate(transferID: transferID, peer: peerState.peer, phase: phase, progress: progress)
        state.lastUpdates[box] = update
        outgoingTransfers[transferID] = state
        for continuation in state.updateSubscribers.values {
            continuation.yield(update)
        }
    }

    private func finishTransferIfDone(id: UUID) {
        guard var state = outgoingTransfers[id], !state.finished, state.started else { return }
        let allTerminal = state.peers.values.allSatisfy { $0.phase.isTerminal }
        let done = state.transferError != nil || (allTerminal && !state.peers.isEmpty)
        guard done else { return }

        state.exportTask?.cancel()
        state.finished = true

        var completed: [Peer] = []
        var declined: [Peer] = []
        var failed: [(peer: Peer, error: LocalSyncError)] = []
        for peerState in state.peers.values {
            switch peerState.phase {
            case .completed: completed.append(peerState.peer)
            case .declined: declined.append(peerState.peer)
            case .failed(let error): failed.append((peerState.peer, error))
            default: failed.append((peerState.peer, .cancelled))
            }
        }
        let summary = TransferSummary(completed: completed, declined: declined, failed: failed)
        completedSummaries[id] = summary

        for continuation in state.updateSubscribers.values {
            continuation.finish()
        }
        state.updateSubscribers = [:]
        for continuation in state.preparationSubscribers.values {
            continuation.finish()
        }
        state.preparationSubscribers = [:]

        let waiters = state.summaryWaiters
        state.summaryWaiters = []
        let transferError = state.transferError

        if let temporary = state.temporaryDirectory {
            try? FileManager.default.removeItem(at: temporary)
        }
        outgoingTransfers[id] = state

        for waiter in waiters {
            if let transferError {
                waiter.resume(throwing: transferError)
            } else {
                waiter.resume(returning: summary)
            }
        }
    }

    func cancelOutgoingTransfer(id: UUID) {
        guard var early = outgoingTransfers[id] else {
            // The transfer's start task hasn't run yet: leave a tombstone so
            // it finishes immediately as cancelled instead of proceeding.
            var placeholder = OutgoingTransferState(id: id)
            placeholder.transferError = .cancelled
            outgoingTransfers[id] = placeholder
            return
        }
        if !early.started {
            early.transferError = .cancelled
            outgoingTransfers[id] = early
            return
        }
        guard let state = outgoingTransfers[id], !state.finished else { return }
        state.exportTask?.cancel()
        if !state.prepared {
            outgoingTransfers[id]?.transferError = .cancelled
            outgoingTransfers[id]?.prepared = true
            finishPreparation(id: id)
        }
        let cancelMessage = try? JSONEncoder().encode(ControlMessage.cancel(transferID: id))
        for (box, peerState) in state.peers where !peerState.phase.isTerminal {
            if let cancelMessage {
                try? transport.send(cancelMessage, to: [box])
            }
            transitionPeer(transferID: id, box: box, to: .failed(.cancelled))
        }
        finishTransferIfDone(id: id)
    }

    // MARK: Subscriptions & waiting (outgoing)

    func transferUpdatesStream(id: UUID) -> AsyncStream<TransferUpdate> {
        let (stream, continuation) = AsyncStream.makeStream(of: TransferUpdate.self)
        var state = outgoingTransfers[id] ?? OutgoingTransferState(id: id)
        for update in state.lastUpdates.values {
            continuation.yield(update)
        }
        if state.finished {
            continuation.finish()
            return stream
        }
        let subscriberID = UUID()
        state.updateSubscribers[subscriberID] = continuation
        outgoingTransfers[id] = state
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removeTransferSubscriber(id: id, subscriberID: subscriberID) }
        }
        return stream
    }

    private func removeTransferSubscriber(id: UUID, subscriberID: UUID) {
        outgoingTransfers[id]?.updateSubscribers.removeValue(forKey: subscriberID)
    }

    func preparationProgressStream(id: UUID) -> AsyncStream<Double> {
        let (stream, continuation) = AsyncStream.makeStream(of: Double.self)
        var state = outgoingTransfers[id] ?? OutgoingTransferState(id: id)
        if state.preparationFraction > 0 {
            continuation.yield(state.preparationFraction)
        }
        if state.prepared || state.finished {
            continuation.finish()
            return stream
        }
        let subscriberID = UUID()
        state.preparationSubscribers[subscriberID] = continuation
        outgoingTransfers[id] = state
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removePreparationSubscriber(id: id, subscriberID: subscriberID) }
        }
        return stream
    }

    private func removePreparationSubscriber(id: UUID, subscriberID: UUID) {
        outgoingTransfers[id]?.preparationSubscribers.removeValue(forKey: subscriberID)
    }

    func awaitSummary(id: UUID) async throws -> TransferSummary {
        if let state = outgoingTransfers[id], state.finished {
            if let error = state.transferError { throw error }
            if let summary = completedSummaries[id] { return summary }
        }
        return try await withCheckedThrowingContinuation { continuation in
            var state = outgoingTransfers[id] ?? OutgoingTransferState(id: id)
            if state.finished {
                if let error = state.transferError {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(returning: completedSummaries[id] ?? TransferSummary(completed: [], declined: [], failed: []))
                }
                return
            }
            state.summaryWaiters.append(continuation)
            outgoingTransfers[id] = state
        }
    }

    // MARK: Sync-state hooks

    /// Store writes are chained on one task so per-(content, peer) state
    /// transitions land in the order they happened.
    private func recordSyncState(
        contentID: String,
        peer: Peer,
        _ operation: @escaping @Sendable (SyncStateStore, String, String) async throws -> Void
    ) {
        guard let store = syncStore else { return }
        let identity = peer.syncIdentity
        let previous = storeWriteChain
        storeWriteChain = Task {
            await previous?.value
            try? await operation(store, contentID, identity)
        }
    }
}

// MARK: - Engine: incoming transfer state machine

struct IncomingOfferState {
    let from: Peer
    let payload: OfferPayload
    var responded = false
    var accepted = false
    var destinationDirectory: URL?
    var receivedFiles: [UUID: ReceivedFile] = [:]
    var itemCompletedBytes: [UUID: Int64] = [:]
    var itemFractions: [UUID: Double] = [:]
    var progressSubscribers: [UUID: AsyncStream<TransferProgress>.Continuation] = [:]
    var fileWaiters: [CheckedContinuation<[ReceivedFile], any Error>] = []
    var failure: LocalSyncError?
    var finished = false
    var receivedFilesSnapshot: [ReceivedFile]?
}

extension SyncEngine {
    func incomingTransfersStream() -> AsyncStream<IncomingTransfer> {
        let id = UUID()
        let (stream, continuation) = AsyncStream.makeStream(of: IncomingTransfer.self)
        for (offerID, offer) in incomingOffers where !offer.responded && !offer.finished {
            continuation.yield(makeIncomingTransfer(id: offerID, offer: offer))
        }
        incomingSubscribers[id] = continuation
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removeIncomingSubscriber(id) }
        }
        return stream
    }

    private func makeIncomingTransfer(id: UUID, offer: IncomingOfferState) -> IncomingTransfer {
        IncomingTransfer(
            id: id,
            from: offer.from,
            kind: offer.payload.kind,
            items: offer.payload.items,
            totalByteCount: offer.payload.totalByteCount,
            engine: self
        )
    }

    func handleOffer(_ payload: OfferPayload, from box: MCPeerIDBox) {
        guard let peer = connected[box] else { return }
        guard incomingOffers[payload.transferID] == nil else { return }
        let offer = IncomingOfferState(from: peer, payload: payload)
        incomingOffers[payload.transferID] = offer
        let transfer = makeIncomingTransfer(id: payload.transferID, offer: offer)
        for continuation in incomingSubscribers.values {
            continuation.yield(transfer)
        }
    }

    func acceptOffer(id: UUID, into directory: URL?) throws -> IncomingTransferSession {
        guard var offer = incomingOffers[id] else { throw LocalSyncError.cancelled }
        if offer.finished, let failure = offer.failure { throw failure }
        guard !offer.responded else { throw LocalSyncError.alreadyResponded }
        offer.responded = true
        offer.accepted = true
        offer.destinationDirectory = directory ?? Self.defaultIncomingDirectory(transferID: id)
        incomingOffers[id] = offer

        guard let data = try? JSONEncoder().encode(ControlMessage.accept(transferID: id)) else {
            throw LocalSyncError.transportError("Could not encode accept message")
        }
        do {
            try transport.send(data, to: [offer.from.box])
        } catch {
            failIncomingOffer(id: id, error: .transportError(error.localizedDescription))
            throw LocalSyncError.transportError(error.localizedDescription)
        }
        return IncomingTransferSession(id: id, engine: self)
    }

    func declineOffer(id: UUID) {
        guard var offer = incomingOffers[id], !offer.responded, !offer.finished else { return }
        offer.responded = true
        offer.finished = true
        incomingOffers[id] = offer
        if let data = try? JSONEncoder().encode(ControlMessage.decline(transferID: id)) {
            try? transport.send(data, to: [offer.from.box])
        }
        finishIncomingOffer(id: id)
    }

    func handleSenderCancelled(transferID: UUID, from box: MCPeerIDBox) {
        guard let offer = incomingOffers[transferID], offer.from.box == box, !offer.finished else { return }
        failIncomingOffer(id: transferID, error: .cancelled)
    }

    // MARK: Incoming resources

    func handleResourceReceiveProgress(from box: MCPeerIDBox, name: String, fraction: Double, completedBytes: Int64, totalBytes: Int64) {
        guard let parsed = WireFormat.parseResourceName(name),
              var offer = incomingOffers[parsed.transferID],
              offer.accepted, !offer.finished,
              offer.from.box == box else { return }
        offer.itemCompletedBytes[parsed.itemID] = completedBytes
        offer.itemFractions[parsed.itemID] = fraction
        incomingOffers[parsed.transferID] = offer
        let progress = incomingProgressSnapshot(offer: offer)
        for continuation in offer.progressSubscribers.values {
            continuation.yield(progress)
        }
    }

    func handleResourceReceiveFinished(from box: MCPeerIDBox, name: String, stagedURL: URL?, error: String?) async {
        guard let parsed = WireFormat.parseResourceName(name),
              var offer = incomingOffers[parsed.transferID],
              offer.accepted, !offer.finished,
              offer.from.box == box,
              let item = offer.payload.items.first(where: { $0.itemID == parsed.itemID })
        else {
            // Unsolicited or unparseable resource: never surface it, remove it.
            if let stagedURL {
                try? FileManager.default.removeItem(at: stagedURL)
            }
            return
        }

        if let error {
            failIncomingOffer(id: parsed.transferID, error: .receiveFailed(error))
            return
        }
        guard let stagedURL else {
            failIncomingOffer(id: parsed.transferID, error: .receiveFailed("No file was delivered"))
            return
        }

        do {
            let destinationDirectory = offer.destinationDirectory ?? Self.defaultIncomingDirectory(transferID: parsed.transferID)
            try FileManager.default.createDirectory(at: destinationDirectory, withIntermediateDirectories: true)
            var destination = destinationDirectory.appendingPathComponent(item.filename, isDirectory: false)
            if FileManager.default.fileExists(atPath: destination.path) {
                let unique = "\(UUID().uuidString)-\(item.filename)"
                destination = destinationDirectory.appendingPathComponent(unique, isDirectory: false)
            }
            try FileManager.default.moveItem(at: stagedURL, to: destination)
            offer.receivedFiles[parsed.itemID] = ReceivedFile(url: destination, metadata: item)
            offer.itemCompletedBytes[parsed.itemID] = item.byteCount ?? 0
            offer.itemFractions[parsed.itemID] = 1.0
            incomingOffers[parsed.transferID] = offer
        } catch {
            try? FileManager.default.removeItem(at: stagedURL)
            failIncomingOffer(id: parsed.transferID, error: .receiveFailed(error.localizedDescription))
            return
        }

        if offer.receivedFiles.count == offer.payload.items.count {
            completeIncomingOffer(id: parsed.transferID)
        } else {
            let progress = incomingProgressSnapshot(offer: offer)
            for continuation in offer.progressSubscribers.values {
                continuation.yield(progress)
            }
        }
    }

    private func incomingProgressSnapshot(offer: IncomingOfferState) -> TransferProgress {
        let completed = offer.itemCompletedBytes.values.reduce(0, +)
        if let total = offer.payload.totalByteCount, total > 0 {
            return TransferProgress(
                fractionCompleted: min(1.0, Double(completed) / Double(total)),
                completedByteCount: completed,
                totalByteCount: total
            )
        }
        let itemCount = max(1, offer.payload.items.count)
        let fractionSum = offer.itemFractions.values.reduce(0, +)
        return TransferProgress(fractionCompleted: min(1.0, fractionSum / Double(itemCount)))
    }

    private func completeIncomingOffer(id: UUID) {
        guard var offer = incomingOffers[id], !offer.finished else { return }
        offer.finished = true
        incomingOffers[id] = offer

        if let data = try? JSONEncoder().encode(ControlMessage.received(transferID: id)) {
            try? transport.send(data, to: [offer.from.box])
        }

        let files = offer.payload.items.compactMap { offer.receivedFiles[$0.itemID] }
        for continuation in offer.progressSubscribers.values {
            continuation.yield(.complete)
        }
        finishIncomingOffer(id: id, files: files)
    }

    func failIncomingOffer(id: UUID, error: LocalSyncError) {
        guard var offer = incomingOffers[id], !offer.finished else { return }
        offer.finished = true
        offer.failure = error
        incomingOffers[id] = offer
        for file in offer.receivedFiles.values {
            try? FileManager.default.removeItem(at: file.url)
        }
        finishIncomingOffer(id: id, files: nil)
    }

    private func finishIncomingOffer(id: UUID, files: [ReceivedFile]? = nil) {
        guard var offer = incomingOffers[id] else { return }
        let subscribers = offer.progressSubscribers
        let waiters = offer.fileWaiters
        let failure = offer.failure
        offer.progressSubscribers = [:]
        offer.fileWaiters = []
        offer.finished = true
        offer.receivedFilesSnapshot = files
        incomingOffers[id] = offer
        for continuation in subscribers.values {
            continuation.finish()
        }
        for waiter in waiters {
            if let files {
                waiter.resume(returning: files)
            } else {
                waiter.resume(throwing: failure ?? LocalSyncError.cancelled)
            }
        }
    }

    func offerProgressStream(id: UUID) -> AsyncStream<TransferProgress> {
        let (stream, continuation) = AsyncStream.makeStream(of: TransferProgress.self)
        guard var offer = incomingOffers[id], !offer.finished else {
            continuation.finish()
            return stream
        }
        let subscriberID = UUID()
        offer.progressSubscribers[subscriberID] = continuation
        incomingOffers[id] = offer
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removeOfferProgressSubscriber(id: id, subscriberID: subscriberID) }
        }
        return stream
    }

    private func removeOfferProgressSubscriber(id: UUID, subscriberID: UUID) {
        incomingOffers[id]?.progressSubscribers.removeValue(forKey: subscriberID)
    }

    func awaitReceivedFiles(id: UUID) async throws -> [ReceivedFile] {
        guard var offer = incomingOffers[id] else { throw LocalSyncError.cancelled }
        if offer.finished {
            if let failure = offer.failure { throw failure }
            if let files = offer.receivedFilesSnapshot { return files }
            throw LocalSyncError.cancelled
        }
        return try await withCheckedThrowingContinuation { continuation in
            offer.fileWaiters.append(continuation)
            incomingOffers[id] = offer
        }
    }

    static func defaultIncomingDirectory(transferID: UUID) -> URL {
        let base = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first
            ?? FileManager.default.temporaryDirectory
        return base
            .appendingPathComponent("LocalFirstSync", isDirectory: true)
            .appendingPathComponent("Incoming", isDirectory: true)
            .appendingPathComponent(transferID.uuidString, isDirectory: true)
    }
}

// MARK: - Photo export

/// Seam over PhotoKit so unit tests never touch the real photo library
/// (which would crash a headless test process on macOS TCC).
protocol AssetExporting: Sendable {
    /// Exports the asset's current file representations into `directory`,
    /// preserving format. `progress` reports 0.0–1.0 across all resources,
    /// including any iCloud-original download.
    func export(
        assetIdentifier: String,
        into directory: URL,
        progress: @escaping @Sendable (Double) -> Void
    ) async throws -> ExportedAsset
}

/// The exported files plus the content kind they represent.
struct ExportedAsset: Sendable {
    let kind: TransferContentKind
    let items: [PreparedItem]
}

/// Exports `PHAsset` file representations via `PHAssetResource`, the only
/// documented path that never re-encodes and streams to disk rather than
/// loading whole videos into memory.
struct PhotoAssetExporter: AssetExporting {
    func export(
        assetIdentifier: String,
        into directory: URL,
        progress: @escaping @Sendable (Double) -> Void
    ) async throws -> ExportedAsset {
        let fetchResult = PHAsset.fetchAssets(withLocalIdentifiers: [assetIdentifier], options: nil)
        guard let asset = fetchResult.firstObject else {
            throw LocalSyncError.assetExportFailed("No photo library asset found for identifier \(assetIdentifier)")
        }

        let resources = PHAssetResource.assetResources(for: asset)
        let selection = try Self.selectResources(for: asset, from: resources)

        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)

        var items: [PreparedItem] = []
        let resourceCount = Double(selection.resources.count)
        for (index, resource) in selection.resources.enumerated() {
            let options = PHAssetResourceRequestOptions()
            // The user's own library may keep originals in iCloud
            // ("Optimize Storage"). Downloading them is a prerequisite of a
            // faithful local transfer and is unrelated to how the transfer
            // itself travels (always the local network, never a server).
            options.isNetworkAccessAllowed = true
            let base = Double(index)
            options.progressHandler = { fraction in
                progress((base + fraction) / resourceCount)
            }

            let filename = resource.originalFilename
            let destination = directory.appendingPathComponent(
                "\(UUID().uuidString)-\(filename)",
                isDirectory: false
            )
            do {
                try await PHAssetResourceManager.default().writeData(
                    for: resource,
                    toFile: destination,
                    options: options
                )
            } catch {
                if let photosError = error as? PHPhotosError, photosError.code == .networkAccessRequired {
                    throw LocalSyncError.iCloudOriginalUnavailable(error.localizedDescription)
                }
                throw LocalSyncError.assetExportFailed(error.localizedDescription)
            }

            let attributes = try? FileManager.default.attributesOfItem(atPath: destination.path)
            let byteCount = (attributes?[.size] as? NSNumber)?.int64Value
            items.append(PreparedItem(
                metadata: TransferItemMetadata(
                    itemID: UUID(),
                    filename: filename,
                    typeIdentifier: resource.uniformTypeIdentifier,
                    photoResourceType: resource.type.rawValue,
                    byteCount: byteCount
                ),
                fileURL: destination
            ))
            progress((base + 1) / resourceCount)
        }

        return ExportedAsset(kind: selection.kind, items: items)
    }

    struct ResourceSelection {
        let kind: TransferContentKind
        let resources: [PHAssetResource]
    }

    /// Picks the current, rendered version of the asset: the full-size
    /// (edited) resource when edits exist, otherwise the original. Live
    /// Photos yield both halves so the receiver can re-pair them.
    static func selectResources(for asset: PHAsset, from resources: [PHAssetResource]) throws -> ResourceSelection {
        func first(_ type: PHAssetResourceType) -> PHAssetResource? {
            resources.first { $0.type == type }
        }

        if asset.mediaSubtypes.contains(.photoLive) {
            let still = first(.fullSizePhoto) ?? first(.photo)
            let paired = first(.fullSizePairedVideo) ?? first(.pairedVideo)
            if let still, let paired {
                return ResourceSelection(kind: .livePhoto, resources: [still, paired])
            }
            if let still {
                return ResourceSelection(kind: .photo, resources: [still])
            }
        }

        switch asset.mediaType {
        case .video:
            if let video = first(.fullSizeVideo) ?? first(.video) {
                return ResourceSelection(kind: .video, resources: [video])
            }
        default:
            if let photo = first(.fullSizePhoto) ?? first(.photo) {
                return ResourceSelection(kind: .photo, resources: [photo])
            }
            // Deferred-processing captures can transiently expose only a
            // lower-quality proxy; send it rather than nothing.
            if let proxy = first(.photoProxy) {
                return ResourceSelection(kind: .photo, resources: [proxy])
            }
        }
        // iCloud Shared Library assets can report zero resources.
        throw LocalSyncError.assetExportFailed(
            "The asset has no transferable file representation (shared-library assets are not supported)"
        )
    }
}

// MARK: - Photo import (receiving side)

/// Saves received photo files back into the photo library, preserving Live
/// Photo pairing. Requires `.readWrite` (or add-only) photo authorization.
public enum PhotoLibraryImporter {
    /// Creates one photo-library asset from the files of one received
    /// transfer (one file for a photo or video; two for a Live Photo).
    ///
    /// - Returns: The new asset's local identifier.
    @discardableResult
    public static func saveToPhotoLibrary(_ files: [ReceivedFile]) async throws -> String {
        guard !files.isEmpty else {
            throw LocalSyncError.receiveFailed("No files to save")
        }
        let placeholder = OSAllocatedUnfairLock<String?>(initialState: nil)
        try await PHPhotoLibrary.shared().performChanges {
            let request = PHAssetCreationRequest.forAsset()
            for file in files {
                let options = PHAssetResourceCreationOptions()
                options.originalFilename = file.metadata.filename
                options.shouldMoveFile = true
                request.addResource(
                    with: Self.resourceType(for: file.metadata),
                    fileURL: file.url,
                    options: options
                )
            }
            let identifier = request.placeholderForCreatedAsset?.localIdentifier
            placeholder.withLock { $0 = identifier }
        }
        guard let identifier = placeholder.withLock({ $0 }) else {
            throw LocalSyncError.receiveFailed("The photo library did not report a created asset")
        }
        return identifier
    }

    static func resourceType(for metadata: TransferItemMetadata) -> PHAssetResourceType {
        if let raw = metadata.photoResourceType, let type = PHAssetResourceType(rawValue: raw) {
            // Proxies saved as .photoProxy lose metadata on some OS versions;
            // saving the same bytes as .photo is lossless and safe.
            if type == .photoProxy { return .photo }
            // Edited "full size" renders become the .photo/.video of the new
            // asset on the receiving device.
            if type == .fullSizePhoto { return .photo }
            if type == .fullSizeVideo { return .video }
            if type == .fullSizePairedVideo { return .pairedVideo }
            return type
        }
        // Fall back on the type identifier for arbitrary files.
        if let typeIdentifier = metadata.typeIdentifier, typeIdentifier.contains("movie") || typeIdentifier.contains("video") {
            return .video
        }
        return .photo
    }
}
