// swift-tools-version: 5.7
import PackageDescription

let package = Package(
  name: "ParkingLot",
  platforms: [
    .macOS(.v12)
  ],
  products: [
    .library(
      name: "ParkingLot",
      targets: ["ParkingLot"]),
    .library(
      name: "Spin",
      targets: ["Spin"]),
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-atomics.git", .upToNextMajor(from: "1.0.0")),
  ],
  targets: [
    .target(
      name: "Spin",
      dependencies: []),
    .target(
      name: "ParkingLot",
      dependencies: [
        .product(name: "Atomics", package: "swift-atomics"),
        "Spin",
      ],
      swiftSettings: [ .unsafeFlags([ "-Xfrontend", "-enable-experimental-move-only" ]) ]),
    .testTarget(
      name: "ParkingLotTests",
      dependencies: ["ParkingLot"]),
  ],
  cLanguageStandard: .c11
)
