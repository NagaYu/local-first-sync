import Foundation
import MultipeerConnectivity

extension LocalSync {
    /// The current package version.
    public static let version = "0.1.0"

    /// The wire-protocol version spoken by this build. Peers exchange this in
    /// their hello message so future versions can negotiate.
    static let protocolVersion = 1
}

// MARK: - Peer

/// A boxed `MCPeerID`. `MCPeerID` is immutable after initialization but is not
/// declared `Sendable` by the SDK, so this wrapper carries it across isolation
/// boundaries. It is never mutated.
final class MCPeerIDBox: @unchecked Sendable, Hashable {
    let peerID: MCPeerID

    init(_ peerID: MCPeerID) {
        self.peerID = peerID
    }

    static func == (lhs: MCPeerIDBox, rhs: MCPeerIDBox) -> Bool {
        lhs.peerID == rhs.peerID
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(peerID)
    }
}

/// A nearby or connected device, as seen by this device.
///
/// A `Peer` is a session-scoped value: it identifies a specific device for the
/// lifetime of the current discovery/connection session. For identity that is
/// stable across app launches (used by ``SyncStateStore``), see ``stableID``.
public struct Peer: Sendable, Hashable, Identifiable {
    /// The user-visible device name, e.g. "Yuta's iPhone".
    public let displayName: String

    /// An identity that is stable across app launches, when known.
    ///
    /// Peers running this package advertise a persisted per-install UUID and
    /// exchange it again in a hello message on connection, so this is
    /// populated for every connected peer. It can be `nil` for a peer that
    /// was merely discovered and is running a different Multipeer
    /// Connectivity implementation.
    public internal(set) var stableID: String?

    /// A session-scoped unique identifier for this peer.
    public var id: String { "\(displayName)#\(UInt(bitPattern: box.peerID.hash))" }

    /// The identity string used to key per-peer sync state: the stable
    /// per-install ID when known, otherwise the display name.
    public var syncIdentity: String { stableID ?? displayName }

    let box: MCPeerIDBox

    init(box: MCPeerIDBox, stableID: String? = nil) {
        self.box = box
        self.displayName = box.peerID.displayName
        self.stableID = stableID
    }

    public static func == (lhs: Peer, rhs: Peer) -> Bool {
        lhs.box == rhs.box
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(box)
    }
}

// MARK: - Targets

/// Which peers an outgoing transfer should be sent to.
public struct TransferTarget: Sendable, ExpressibleByArrayLiteral {
    enum Kind: Sendable {
        case allConnectedPeers
        case peers([Peer])
    }

    let kind: Kind

    /// Every peer connected to the session at the moment the send begins.
    public static let allConnectedPeers = TransferTarget(kind: .allConnectedPeers)

    /// A specific set of peers, all of which must currently be connected.
    public static func peers(_ peers: [Peer]) -> TransferTarget {
        TransferTarget(kind: .peers(peers))
    }

    public init(arrayLiteral elements: Peer...) {
        self.init(kind: .peers(elements))
    }

    private init(kind: Kind) {
        self.kind = kind
    }
}

// MARK: - Progress & transfer updates

/// A point-in-time snapshot of one peer's transfer progress.
///
/// This is a value snapshot rather than a live `Progress` object so it can be
/// freely passed between tasks and rendered without KVO.
public struct TransferProgress: Sendable, Hashable {
    /// Fraction of the transfer completed for this peer, from 0.0 to 1.0.
    public let fractionCompleted: Double
    /// Bytes confirmed sent to (or received from) this peer, when known.
    public let completedByteCount: Int64?
    /// Total bytes for this transfer, when known.
    public let totalByteCount: Int64?

    public init(fractionCompleted: Double, completedByteCount: Int64? = nil, totalByteCount: Int64? = nil) {
        self.fractionCompleted = fractionCompleted
        self.completedByteCount = completedByteCount
        self.totalByteCount = totalByteCount
    }

    /// Progress representing "not started".
    public static let none = TransferProgress(fractionCompleted: 0)
    /// Progress representing "fully complete".
    public static let complete = TransferProgress(fractionCompleted: 1)
}

/// The per-peer lifecycle of an outgoing transfer.
public enum TransferPhase: Sendable {
    /// The offer was sent; waiting for the peer's user to accept or decline.
    case waitingForAcceptance
    /// The peer declined the transfer. Terminal.
    case declined
    /// Resource data is flowing to this peer.
    case sending
    /// Every byte was delivered and the peer confirmed it stored the files. Terminal.
    case completed
    /// The transfer to this peer failed. Other peers are unaffected. Terminal.
    case failed(LocalSyncError)

    /// Whether this phase ends the transfer for its peer.
    public var isTerminal: Bool {
        switch self {
        case .declined, .completed, .failed: true
        case .waitingForAcceptance, .sending: false
        }
    }
}

/// One update about one peer's part of an outgoing transfer.
///
/// A broadcast to three peers produces three independent series of updates —
/// one per peer — never a blended average. If the iPad finishes while the Mac
/// is still at 40%, you will see exactly that.
public struct TransferUpdate: Sendable {
    /// The transfer this update belongs to.
    public let transferID: UUID
    /// The peer this update is about.
    public let peer: Peer
    /// Where this peer is in the transfer lifecycle.
    public let phase: TransferPhase
    /// This peer's own progress. Meaningful during ``TransferPhase/sending``.
    public let progress: TransferProgress
}

/// Terminal outcome of an outgoing transfer, per peer.
public struct TransferSummary: Sendable {
    /// Peers that received and confirmed the transfer.
    public let completed: [Peer]
    /// Peers that declined the offer.
    public let declined: [Peer]
    /// Peers whose transfer failed, with the reason.
    public let failed: [(peer: Peer, error: LocalSyncError)]

    /// True when at least one peer completed and none are outstanding.
    public var anySucceeded: Bool { !completed.isEmpty }
}

// MARK: - Items and metadata

/// Describes one file within a transfer (a plain photo is one item; a Live
/// Photo is two — the still and the paired video).
public struct TransferItemMetadata: Sendable, Hashable, Codable {
    /// Identifies the item within its transfer.
    public let itemID: UUID
    /// The original filename, suitable for recreating the file.
    public let filename: String
    /// A uniform type identifier string describing the file format, when known.
    public let typeIdentifier: String?
    /// Raw `PHAssetResourceType` value when this item came from a photo
    /// library asset, so the receiver can rebuild the asset faithfully.
    public let photoResourceType: Int?
    /// File size in bytes, when known before sending.
    public let byteCount: Int64?
}

/// What kind of content a transfer carries.
public enum TransferContentKind: String, Sendable, Codable {
    /// A still photo from the photo library.
    case photo
    /// A Live Photo (still + paired video).
    case livePhoto
    /// A video from the photo library.
    case video
    /// An arbitrary file.
    case file
}

// MARK: - Events

/// Session-level events: peers appearing, disappearing, connecting, disconnecting.
public enum SyncEvent: Sendable {
    /// A nearby device advertising the same service was found.
    case peerFound(Peer)
    /// A previously found device is no longer visible.
    case peerLost(Peer)
    /// A peer finished connecting and is now part of the session.
    case peerConnected(Peer)
    /// A connected peer left the session or the link dropped.
    case peerDisconnected(Peer)
}

// MARK: - Errors

/// Errors thrown by ``LocalSync`` and related types.
public enum LocalSyncError: Error, Sendable {
    /// The service name is not a valid Bonjour service type. Must be 1–15
    /// characters: lowercase ASCII letters, digits, and hyphens, containing at
    /// least one letter, not beginning or ending with a hyphen, and with no
    /// adjacent hyphens.
    case invalidServiceName(String, reason: String)
    /// `connect(to:)` did not reach the connected state in time.
    case connectionTimedOut(Peer)
    /// The peer refused the connection or the link could not be established.
    case connectionFailed(Peer)
    /// The operation needs a connected peer, but this peer is not connected.
    case peerNotConnected(Peer)
    /// A send targeted `.allConnectedPeers` while no peers were connected.
    case noPeersConnected
    /// The receiving peer never answered the transfer offer.
    case offerTimedOut
    /// The transfer was cancelled locally.
    case cancelled
    /// The underlying Multipeer Connectivity transfer failed.
    case transportError(String)
    /// The photo library asset could not be exported for sending.
    case assetExportFailed(String)
    /// The asset's original bytes are in iCloud and could not be downloaded.
    /// This concerns the *user's own* iCloud Photos setting — the transfer
    /// between devices never touches a server.
    case iCloudOriginalUnavailable(String)
    /// The received files could not be moved into place.
    case receiveFailed(String)
    /// Every byte arrived but the receiving peer never confirmed storage.
    case confirmationMissing
    /// This transfer handle was already responded to (accepted or declined).
    case alreadyResponded
}

extension LocalSyncError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .invalidServiceName(let name, let reason):
            "Invalid service name \"\(name)\": \(reason)"
        case .connectionTimedOut(let peer):
            "Connecting to \(peer.displayName) timed out."
        case .connectionFailed(let peer):
            "Could not connect to \(peer.displayName)."
        case .peerNotConnected(let peer):
            "\(peer.displayName) is not connected."
        case .noPeersConnected:
            "No peers are connected."
        case .offerTimedOut:
            "The peer did not respond to the transfer offer."
        case .cancelled:
            "The transfer was cancelled."
        case .transportError(let message):
            "Transfer failed: \(message)"
        case .assetExportFailed(let message):
            "Could not export the photo: \(message)"
        case .iCloudOriginalUnavailable(let message):
            "The original is in iCloud and could not be downloaded: \(message)"
        case .receiveFailed(let message):
            "Could not store the received files: \(message)"
        case .confirmationMissing:
            "The peer received the data but never confirmed storing it."
        case .alreadyResponded:
            "This request was already accepted or declined."
        }
    }
}

// MARK: - Configuration

/// Tunable timeouts and behavior for a ``LocalSync`` session.
public struct LocalSyncConfiguration: Sendable {
    /// How long `connect(to:)` waits for the connection to establish.
    public var connectTimeout: TimeInterval = 30
    /// How long a sender waits for the receiving side to accept or decline an
    /// offer before treating it as failed. Generous by default because a
    /// human is expected to tap "Accept".
    public var acceptTimeout: TimeInterval = 120
    /// How long the sender waits for the receiver's storage confirmation
    /// after the last byte is delivered.
    public var confirmationTimeout: TimeInterval = 15

    public init() {}
}

// MARK: - Service-name validation

extension LocalSync {
    /// Validates a Multipeer Connectivity service name against Bonjour
    /// service-type rules (RFC 6335 §5.1, as enforced by the framework):
    /// 1–15 characters; lowercase ASCII letters, digits, and hyphens only; at
    /// least one letter; no leading/trailing hyphen; no adjacent hyphens.
    ///
    /// - Throws: ``LocalSyncError/invalidServiceName(_:reason:)``
    public static func validateServiceName(_ name: String) throws {
        func fail(_ reason: String) throws -> Never {
            throw LocalSyncError.invalidServiceName(name, reason: reason)
        }
        if name.isEmpty || name.count > 15 {
            try fail("must be 1–15 characters, got \(name.count)")
        }
        var hasLetter = false
        var previousWasHyphen = false
        for scalar in name.unicodeScalars {
            switch scalar {
            case "a"..."z":
                hasLetter = true
                previousWasHyphen = false
            case "0"..."9":
                previousWasHyphen = false
            case "-":
                if previousWasHyphen {
                    try fail("must not contain adjacent hyphens")
                }
                previousWasHyphen = true
            default:
                try fail("character \"\(scalar)\" is not allowed; use lowercase ASCII letters, digits, and hyphens")
            }
        }
        if !hasLetter {
            try fail("must contain at least one ASCII letter")
        }
        if name.hasPrefix("-") || name.hasSuffix("-") {
            try fail("must not begin or end with a hyphen")
        }
    }
}
