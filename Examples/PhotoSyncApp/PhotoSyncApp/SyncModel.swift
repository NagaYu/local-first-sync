import Foundation
import LocalFirstSync
import Observation
import Photos
import PhotosUI
import SwiftUI

/// One outgoing broadcast, with an independent progress row per target peer —
/// never a blended average.
@MainActor
@Observable
final class OutgoingRow: Identifiable {
    struct PeerRow: Identifiable {
        var id: String { peerName }
        let peerName: String
        var phaseLabel: String
        var fraction: Double
        var finished: Bool
    }

    let id: UUID
    let label: String
    var preparationFraction: Double?
    var peerRows: [PeerRow] = []

    init(transfer: Transfer, label: String) {
        self.id = transfer.id
        self.label = label

        Task {
            for await fraction in await transfer.preparationProgress() {
                self.preparationFraction = fraction
            }
            self.preparationFraction = nil
        }
        Task {
            for await update in transfer {
                self.apply(update)
            }
        }
    }

    private func apply(_ update: TransferUpdate) {
        let (phaseLabel, finished): (String, Bool) = switch update.phase {
        case .waitingForAcceptance: ("Waiting for accept…", false)
        case .sending: ("Sending", false)
        case .declined: ("Declined", true)
        case .completed: ("✓ Synced", true)
        case .failed(let error): ("Failed: \(error.localizedDescription)", true)
        }
        let row = PeerRow(
            peerName: update.peer.displayName,
            phaseLabel: phaseLabel,
            fraction: update.phase.isTerminal ? 1 : update.progress.fractionCompleted,
            finished: finished
        )
        if let index = peerRows.firstIndex(where: { $0.peerName == update.peer.displayName }) {
            peerRows[index] = row
        } else {
            peerRows.append(row)
        }
    }
}

/// One incoming offer: explicitly accept or decline, watch progress, then
/// save into the photo library.
@MainActor
@Observable
final class IncomingRow: Identifiable {
    let transfer: IncomingTransfer
    nonisolated var id: UUID { transfer.id }
    var stateLabel = "Offered"
    var fraction: Double = 0
    var files: [ReceivedFile] = []
    var answered = false
    var savedToLibrary = false

    init(transfer: IncomingTransfer) {
        self.transfer = transfer
    }

    var summary: String {
        let names = transfer.items.map(\.filename).joined(separator: ", ")
        let size = transfer.totalByteCount.map { " (\(ByteCountFormatter.string(fromByteCount: $0, countStyle: .file)))" } ?? ""
        return "\(names)\(size) from \(transfer.from.displayName)"
    }

    func accept() {
        guard !answered else { return }
        answered = true
        stateLabel = "Receiving…"
        Task {
            do {
                let session = try await transfer.accept()
                Task {
                    for await progress in await session.progress() {
                        self.fraction = progress.fractionCompleted
                    }
                }
                files = try await session.files()
                stateLabel = "Received"
            } catch {
                stateLabel = "Failed: \(error.localizedDescription)"
            }
        }
    }

    func decline() {
        guard !answered else { return }
        answered = true
        stateLabel = "Declined"
        Task { await transfer.decline() }
    }

    func saveToPhotoLibrary() {
        guard !files.isEmpty, !savedToLibrary else { return }
        Task {
            let status = await PHPhotoLibrary.requestAuthorization(for: .addOnly)
            guard status == .authorized || status == .limited else {
                stateLabel = "Photo access denied"
                return
            }
            do {
                try await PhotoLibraryImporter.saveToPhotoLibrary(files)
                savedToLibrary = true
                stateLabel = "Saved to Photos"
            } catch {
                stateLabel = "Save failed: \(error.localizedDescription)"
            }
        }
    }
}

/// The app's connection to `LocalSync`: consumes its streams into observable
/// state the views render.
@MainActor
@Observable
final class SyncModel {
    let sync = LocalSync(serviceName: "lfs-photosync")

    private(set) var nearbyPeers: [Peer] = []
    private(set) var connectionRequests: [ConnectionRequest] = []
    private(set) var connectedPeers: [Peer] = []
    private(set) var outgoing: [OutgoingRow] = []
    private(set) var incoming: [IncomingRow] = []
    private(set) var syncRecords: [SyncRecord] = []
    private(set) var statusMessage: String?

    private var store: SyncStateStore?
    private var started = false

    func start() async {
        guard !started else { return }
        started = true

        do {
            try await sync.startAdvertising()
            try await sync.startDiscovery()
        } catch {
            statusMessage = error.localizedDescription
        }

        // Track per-(photo, device) sync state on disk.
        if let store = try? await SyncStateStore.onDisk() {
            self.store = store
            await sync.attachSyncState(store)
            await refreshRecords()
        }

        Task {
            for await peer in sync.nearbyPeers() {
                if !nearbyPeers.contains(peer) {
                    nearbyPeers.append(peer)
                }
            }
        }
        Task {
            for await event in sync.events() {
                switch event {
                case .peerLost(let peer):
                    nearbyPeers.removeAll { $0 == peer }
                case .peerFound, .peerConnected, .peerDisconnected:
                    break
                }
                connectedPeers = await sync.connectedPeers
            }
        }
        Task {
            for await request in sync.connectionRequests() {
                connectionRequests.append(request)
            }
        }
        Task {
            for await transfer in sync.incomingTransfers() {
                incoming.insert(IncomingRow(transfer: transfer), at: 0)
            }
        }
    }

    // MARK: Connections

    func connect(to peer: Peer) {
        Task {
            do {
                try await sync.connect(to: peer)
                statusMessage = nil
            } catch {
                statusMessage = "Connect failed: \(error.localizedDescription)"
            }
            connectedPeers = await sync.connectedPeers
        }
    }

    func respond(to request: ConnectionRequest, accept: Bool) {
        connectionRequests.removeAll { $0.id == request.id }
        Task {
            if accept {
                try? await request.accept()
            } else {
                await request.decline()
            }
            connectedPeers = await sync.connectedPeers
        }
    }

    // MARK: Sending

    /// Broadcasts each picked photo to every connected device at once.
    func send(items: [PhotosPickerItem]) {
        for item in items {
            guard let identifier = item.itemIdentifier else { continue }
            let assets = PHAsset.fetchAssets(withLocalIdentifiers: [identifier], options: nil)
            guard let asset = assets.firstObject else { continue }
            let transfer = sync.send(photo: asset, to: .allConnectedPeers)
            outgoing.insert(OutgoingRow(transfer: transfer, label: identifier), at: 0)
            Task {
                _ = try? await transfer.waitUntilCompleted()
                await refreshRecords()
            }
        }
    }

    // MARK: Sync state

    func refreshRecords() async {
        guard let store else { return }
        syncRecords = (try? await store.allRecords()) ?? []
    }
}
