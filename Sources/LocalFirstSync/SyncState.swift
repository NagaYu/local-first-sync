import Foundation
import SwiftData

// MARK: - Public value types

/// Where one (content, peer) pair stands. "Synced with the Mac" and "synced
/// with the iPad" are independent records — a photo can be `.synced` with one
/// peer and `.failed` with another at the same time.
public enum SyncStatus: String, Sendable, Codable, CaseIterable {
    /// Offered to the peer; awaiting their accept.
    case pending
    /// The peer accepted; bytes are in flight.
    case inFlight
    /// The peer confirmed it received and stored the content. Terminal-good.
    case synced
    /// The transfer to this peer failed or was interrupted. Retryable.
    case failed
    /// The peer declined the offer.
    case declined
}

/// A snapshot of one (content, peer) pair's sync state.
///
/// This is a Sendable value copy, not a live SwiftData object — safe to hold,
/// pass between tasks, and render.
public struct SyncRecord: Sendable, Hashable, Identifiable {
    /// Identifies the content: a `PHAsset` local identifier for photos, the
    /// source URL string for files, or any key your app chooses.
    public let contentID: String
    /// The peer's stable identity (see ``Peer/syncIdentity``).
    public let peerID: String
    /// Current status of this pair.
    public let status: SyncStatus
    /// When this pair last changed state.
    public let updatedAt: Date
    /// The most recent failure reason, if any.
    public let lastError: String?
    /// How many transfer attempts have been made for this pair.
    public let attemptCount: Int

    public var id: String { "\(contentID)|\(peerID)" }
}

// MARK: - Schema

/// Versioned from day one: consumers' on-disk stores outlive package
/// releases, and retrofitting a `VersionedSchema` after shipping is painful.
enum SyncSchemaV1: VersionedSchema {
    static var versionIdentifier: Schema.Version { Schema.Version(1, 0, 0) }
    static var models: [any PersistentModel.Type] { [SyncRecordModel.self] }

    @Model
    final class SyncRecordModel {
        // Uniqueness of (contentID, peerID) is enforced by fetch-then-update
        // inside the single `SyncStateStore` actor rather than by
        // `@Attribute(.unique)`: composite unique constraints don't exist on
        // the iOS 17 floor, and single-property upsert has documented
        // crash/duplicate bugs there.
        var contentID: String = ""
        var peerID: String = ""
        var statusRaw: String = SyncStatus.pending.rawValue
        var updatedAt: Date = Date()
        var lastError: String?
        var attemptCount: Int = 0

        init(contentID: String, peerID: String, status: SyncStatus) {
            self.contentID = contentID
            self.peerID = peerID
            self.statusRaw = status.rawValue
            self.updatedAt = Date()
        }

        var snapshot: SyncRecord {
            SyncRecord(
                contentID: contentID,
                peerID: peerID,
                status: SyncStatus(rawValue: statusRaw) ?? .failed,
                updatedAt: updatedAt,
                lastError: lastError,
                attemptCount: attemptCount
            )
        }
    }
}

typealias SyncRecordModel = SyncSchemaV1.SyncRecordModel

// MARK: - Store

/// SwiftData-backed tracking of sync status per (content, peer) pair.
///
/// One long-lived instance per store; every read and write funnels through
/// this actor, which is what makes the fetch-then-update upsert race-free.
/// An interrupted transfer to one peer updates only that peer's record —
/// other peers' rows are untouched by design (they are separate rows).
///
/// Attach a store to a ``LocalSync`` with ``LocalSync/attachSyncState(_:)``
/// and it records every outgoing transfer's per-peer lifecycle
/// automatically; the query methods are also usable standalone.
@ModelActor
public actor SyncStateStore {
    // MARK: Factories

    /// A store that keeps everything in memory. Ideal for tests and previews.
    public static func inMemory() async throws -> SyncStateStore {
        try await makeDetached(storeURL: nil)
    }

    /// A store persisted at the given location, or at a default location in
    /// Application Support when `url` is nil.
    public static func onDisk(at url: URL? = nil) async throws -> SyncStateStore {
        let storeURL: URL
        if let url {
            storeURL = url
        } else {
            let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
                ?? FileManager.default.temporaryDirectory
            let directory = base.appendingPathComponent("LocalFirstSync", isDirectory: true)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            storeURL = directory.appendingPathComponent("SyncState.store", isDirectory: false)
        }
        return try await makeDetached(storeURL: storeURL)
    }

    /// Builds the container and actor off the caller's actor: a ModelActor's
    /// executor binds to the context it was created in, so constructing on
    /// the main actor would silently pin all store work to the main thread.
    ///
    /// The closure captures only a Sendable `URL?` and builds the
    /// `ModelConfiguration` inside — Swift 6.1's isolation checking can't
    /// prove a captured configuration disconnected, so it must not cross.
    private static func makeDetached(storeURL: URL?) async throws -> SyncStateStore {
        try await Task.detached {
            let schema = Schema(versionedSchema: SyncSchemaV1.self)
            let configuration: ModelConfiguration
            if let storeURL {
                configuration = ModelConfiguration(url: storeURL)
            } else {
                configuration = ModelConfiguration(isStoredInMemoryOnly: true)
            }
            let container = try ModelContainer(for: schema, configurations: [configuration])
            return SyncStateStore(modelContainer: container)
        }.value
    }

    // MARK: Writes (one per lifecycle transition)

    /// Records that `content` was offered to `peer`.
    public func markPending(content: String, peer: String) throws {
        try upsert(content: content, peer: peer) { record in
            record.statusRaw = SyncStatus.pending.rawValue
            record.lastError = nil
        }
    }

    /// Records that `peer` accepted and bytes are flowing. Counts an attempt.
    public func markInFlight(content: String, peer: String) throws {
        try upsert(content: content, peer: peer) { record in
            record.statusRaw = SyncStatus.inFlight.rawValue
            record.attemptCount += 1
            record.lastError = nil
        }
    }

    /// Records that `peer` confirmed receipt. Call only once that specific
    /// peer's transfer is confirmed complete — never for a broadcast as a
    /// whole.
    public func markSynced(content: String, peer: String) throws {
        try upsert(content: content, peer: peer) { record in
            record.statusRaw = SyncStatus.synced.rawValue
            record.lastError = nil
        }
    }

    /// Records a failure (including interruption) for exactly this pair.
    public func markFailed(content: String, peer: String, reason: String?) throws {
        try upsert(content: content, peer: peer) { record in
            record.statusRaw = SyncStatus.failed.rawValue
            record.lastError = reason
        }
    }

    /// Records that `peer` declined the offer.
    public func markDeclined(content: String, peer: String) throws {
        try upsert(content: content, peer: peer) { record in
            record.statusRaw = SyncStatus.declined.rawValue
        }
    }

    /// Deletes the record for this pair, if any.
    public func reset(content: String, peer: String) throws {
        if let model = try fetchModel(content: content, peer: peer) {
            modelContext.delete(model)
            try modelContext.save()
        }
    }

    // MARK: Queries

    /// The record for one (content, peer) pair, or nil if never tracked.
    public func record(content: String, peer: String) throws -> SyncRecord? {
        try fetchModel(content: content, peer: peer)?.snapshot
    }

    /// All peers' records for one piece of content — e.g. "synced with the
    /// Mac, still pending on the iPad".
    public func records(forContent contentID: String) throws -> [SyncRecord] {
        let descriptor = FetchDescriptor<SyncRecordModel>(
            predicate: #Predicate { $0.contentID == contentID },
            sortBy: [SortDescriptor(\.peerID)]
        )
        return try modelContext.fetch(descriptor).map(\.snapshot)
    }

    /// All content records for one peer.
    public func records(forPeer peerID: String) throws -> [SyncRecord] {
        let descriptor = FetchDescriptor<SyncRecordModel>(
            predicate: #Predicate { $0.peerID == peerID },
            sortBy: [SortDescriptor(\.updatedAt, order: .reverse)]
        )
        return try modelContext.fetch(descriptor).map(\.snapshot)
    }

    /// Content that has not reached `.synced` with the given peer, from the
    /// supplied candidates — the "what still needs sending to the iPad" query.
    public func contentIDs(notSyncedWith peerID: String, from candidates: [String]) throws -> [String] {
        let synced = try records(forPeer: peerID)
            .filter { $0.status == .synced }
            .map(\.contentID)
        let syncedSet = Set(synced)
        return candidates.filter { !syncedSet.contains($0) }
    }

    /// Every record in the store.
    public func allRecords() throws -> [SyncRecord] {
        let descriptor = FetchDescriptor<SyncRecordModel>(
            sortBy: [SortDescriptor(\.updatedAt, order: .reverse)]
        )
        return try modelContext.fetch(descriptor).map(\.snapshot)
    }

    // MARK: Internals

    private func fetchModel(content: String, peer: String) throws -> SyncRecordModel? {
        var descriptor = FetchDescriptor<SyncRecordModel>(
            predicate: #Predicate { $0.contentID == content && $0.peerID == peer }
        )
        descriptor.fetchLimit = 1
        return try modelContext.fetch(descriptor).first
    }

    private func upsert(content: String, peer: String, mutate: (SyncRecordModel) -> Void) throws {
        let model: SyncRecordModel
        if let existing = try fetchModel(content: content, peer: peer) {
            model = existing
        } else {
            model = SyncRecordModel(contentID: content, peerID: peer, status: .pending)
            modelContext.insert(model)
        }
        mutate(model)
        model.updatedAt = Date()
        try modelContext.save()
    }
}
