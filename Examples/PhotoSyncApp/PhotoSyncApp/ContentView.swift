import LocalFirstSync
import PhotosUI
import SwiftUI

struct ContentView: View {
    @Environment(SyncModel.self) private var model

    var body: some View {
        TabView {
            DevicesView()
                .tabItem { Label("Devices", systemImage: "macbook.and.iphone") }
            SendView()
                .tabItem { Label("Send", systemImage: "photo.on.rectangle.angled") }
            ActivityView()
                .tabItem { Label("Activity", systemImage: "arrow.down.circle") }
        }
    }
}

// MARK: - Devices

/// Discovery and connection: every device the user owns can join the same
/// session. Connecting always requires an explicit accept on the other side.
struct DevicesView: View {
    @Environment(SyncModel.self) private var model

    var body: some View {
        NavigationStack {
            List {
                if let message = model.statusMessage {
                    Section {
                        Text(message).foregroundStyle(.red)
                    }
                }

                Section("Connected (\(model.connectedPeers.count))") {
                    if model.connectedPeers.isEmpty {
                        Text("No devices connected yet")
                            .foregroundStyle(.secondary)
                    }
                    ForEach(model.connectedPeers) { peer in
                        Label(peer.displayName, systemImage: "checkmark.circle.fill")
                            .foregroundStyle(.green)
                    }
                }

                if !model.connectionRequests.isEmpty {
                    Section("Wants to connect") {
                        ForEach(model.connectionRequests, id: \.id) { request in
                            HStack {
                                Text(request.from.displayName)
                                Spacer()
                                Button("Decline") { model.respond(to: request, accept: false) }
                                    .buttonStyle(.bordered)
                                Button("Accept") { model.respond(to: request, accept: true) }
                                    .buttonStyle(.borderedProminent)
                            }
                        }
                    }
                }

                Section("Nearby") {
                    let candidates = model.nearbyPeers.filter { peer in
                        !model.connectedPeers.contains(peer)
                    }
                    if candidates.isEmpty {
                        Text("Searching for nearby devices…")
                            .foregroundStyle(.secondary)
                    }
                    ForEach(candidates) { peer in
                        HStack {
                            Text(peer.displayName)
                            Spacer()
                            Button("Connect") { model.connect(to: peer) }
                                .buttonStyle(.borderedProminent)
                        }
                    }
                }
            }
            .navigationTitle("PhotoSync")
        }
    }
}

// MARK: - Send

/// Pick photos and broadcast them to every connected device, with one
/// progress bar per device — if the iPad finishes while the Mac is still
/// transferring, that's exactly what you see.
struct SendView: View {
    @Environment(SyncModel.self) private var model
    @State private var selection: [PhotosPickerItem] = []

    var body: some View {
        NavigationStack {
            List {
                Section {
                    PhotosPicker(
                        selection: $selection,
                        maxSelectionCount: 10,
                        matching: .any(of: [.images, .videos]),
                        photoLibrary: .shared()
                    ) {
                        Label("Choose photos", systemImage: "photo.badge.plus")
                    }
                    .onChange(of: selection) { _, items in
                        guard !items.isEmpty else { return }
                        model.send(items: items)
                        selection = []
                    }

                    if model.connectedPeers.isEmpty {
                        Text("Connect at least one device first")
                            .foregroundStyle(.secondary)
                    } else {
                        Text("Sends to all \(model.connectedPeers.count) connected device(s); each must accept")
                            .font(.footnote)
                            .foregroundStyle(.secondary)
                    }
                }

                ForEach(model.outgoing) { row in
                    Section(row.label) {
                        if let preparation = row.preparationFraction {
                            VStack(alignment: .leading) {
                                Text("Preparing (downloading original if needed)…")
                                    .font(.footnote)
                                ProgressView(value: preparation)
                            }
                        }
                        ForEach(row.peerRows) { peerRow in
                            VStack(alignment: .leading, spacing: 4) {
                                HStack {
                                    Text(peerRow.peerName).font(.callout)
                                    Spacer()
                                    Text(peerRow.phaseLabel)
                                        .font(.footnote)
                                        .foregroundStyle(.secondary)
                                }
                                ProgressView(value: peerRow.fraction)
                                    .tint(peerRow.finished ? .green : .accentColor)
                            }
                        }
                    }
                }
            }
            .navigationTitle("Send")
        }
    }
}

// MARK: - Activity

/// Incoming offers (explicit accept/decline, like AirDrop) and the
/// per-(photo, device) sync ledger.
struct ActivityView: View {
    @Environment(SyncModel.self) private var model

    var body: some View {
        NavigationStack {
            List {
                Section("Incoming") {
                    if model.incoming.isEmpty {
                        Text("Offers from other devices appear here")
                            .foregroundStyle(.secondary)
                    }
                    ForEach(model.incoming) { row in
                        IncomingRowView(row: row)
                    }
                }

                Section("Sync status (per photo, per device)") {
                    if model.syncRecords.isEmpty {
                        Text("Nothing tracked yet")
                            .foregroundStyle(.secondary)
                    }
                    ForEach(model.syncRecords) { record in
                        HStack {
                            VStack(alignment: .leading) {
                                Text(record.contentID)
                                    .font(.caption)
                                    .lineLimit(1)
                                Text("→ \(record.peerID)")
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(1)
                            }
                            Spacer()
                            StatusBadge(status: record.status)
                        }
                    }
                }
            }
            .navigationTitle("Activity")
            .refreshable { await model.refreshRecords() }
        }
    }
}

struct IncomingRowView: View {
    @Bindable var row: IncomingRow

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(row.summary).font(.callout)
            HStack {
                Text(row.stateLabel)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                Spacer()
                if !row.answered {
                    Button("Decline") { row.decline() }
                        .buttonStyle(.bordered)
                    Button("Accept") { row.accept() }
                        .buttonStyle(.borderedProminent)
                } else if !row.files.isEmpty, !row.savedToLibrary {
                    Button("Save to Photos") { row.saveToPhotoLibrary() }
                        .buttonStyle(.borderedProminent)
                }
            }
            if row.answered, row.files.isEmpty, row.fraction > 0, row.fraction < 1 {
                ProgressView(value: row.fraction)
            }
        }
        .padding(.vertical, 2)
    }
}

struct StatusBadge: View {
    let status: SyncStatus

    var body: some View {
        Text(label)
            .font(.caption2.weight(.semibold))
            .padding(.horizontal, 8)
            .padding(.vertical, 3)
            .background(color.opacity(0.15), in: Capsule())
            .foregroundStyle(color)
    }

    private var label: String {
        switch status {
        case .pending: "Pending"
        case .inFlight: "Sending"
        case .synced: "Synced"
        case .failed: "Failed"
        case .declined: "Declined"
        }
    }

    private var color: Color {
        switch status {
        case .pending: .orange
        case .inFlight: .blue
        case .synced: .green
        case .failed: .red
        case .declined: .gray
        }
    }
}
