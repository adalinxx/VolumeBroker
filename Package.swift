// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "VolumeBroker",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [
        .library(name: "VolumeBroker", targets: ["VolumeBroker"]),
    ],
    dependencies: [
        .package(url: "https://github.com/adalinxx/cashew.git", from: "3.0.0"),
        .package(url: "https://github.com/adalinxx/ArrayTrie.git", from: "1.0.0"),
    ],
    targets: [
        .target(
            name: "VolumeBrokerSQLite",
            publicHeadersPath: ".",
            linkerSettings: [.linkedLibrary("sqlite3")]
        ),
        .target(
            name: "VolumeBroker",
            dependencies: [
                "VolumeBrokerSQLite",
                .product(name: "cashew", package: "cashew"),
                .product(name: "ArrayTrie", package: "ArrayTrie"),
            ]
        ),
        .testTarget(
            name: "VolumeBrokerTests",
            dependencies: ["VolumeBroker"]
        ),
        .testTarget(
            name: "VolumeBrokerBenchmarks",
            dependencies: ["VolumeBroker"]
        ),
    ]
)
