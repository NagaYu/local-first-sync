// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "local-first-sync",
    platforms: [
        // SwiftData sets the floor: iOS 17 / macOS 14.
        // MultipeerConnectivity and Photos are available well below these minimums
        // and behave identically on iPhone, iPad, and Mac.
        .iOS(.v17),
        .macOS(.v14),
        .visionOS(.v1),
    ],
    products: [
        .library(name: "LocalFirstSync", targets: ["LocalFirstSync"])
    ],
    targets: [
        .target(
            name: "LocalFirstSync",
            path: "Sources/LocalFirstSync"
        ),
        .testTarget(
            name: "LocalFirstSyncTests",
            dependencies: ["LocalFirstSync"],
            path: "Tests/LocalFirstSyncTests"
        ),
    ]
)
