// swift-tools-version:5.3

import PackageDescription

let package = Package(
    name: "SQLiteCombine",
    platforms: [
        .iOS(.v13),
        .macOS(.v10_15),
        .tvOS(.v13),
        .watchOS(.v6),
    ],
    products: [
        .library(
            name: "SQLiteCombine",
            targets: ["SQLiteCombine"]),
    ],
    targets: [
        .target(
            name: "SQLiteCombine",
            dependencies: []),
        .testTarget(
            name: "SQLiteCombineTests",
            dependencies: ["SQLiteCombine"]),
    ]
)
