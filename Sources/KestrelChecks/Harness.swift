import Foundation

/// Minimal check harness.
///
/// Command Line Tools ships neither XCTest nor swift-testing, so `swift test`
/// cannot build on this machine. Checks run as a plain executable instead:
/// `swift run KestrelChecks` prints one line per check and exits non-zero if any
/// of them failed.
///
/// A reference type, because top-level code is main-actor isolated and cannot
/// call `mutating async` methods on a global value.
@MainActor
final class Harness {
    private var passed = 0
    private var failures: [String] = []

    func check(_ name: String, _ body: () throws -> Void) {
        do {
            try body()
            record(pass: name)
        } catch {
            record(failure: name, error)
        }
    }

    func checkAsync(_ name: String, _ body: @MainActor () async throws -> Void) async {
        do {
            try await body()
            record(pass: name)
        } catch {
            record(failure: name, error)
        }
    }

    func suite(_ name: String, _ body: (Harness) -> Void) {
        print("\n\(name)")
        body(self)
    }

    func asyncSuite(_ name: String, _ body: @MainActor (Harness) async -> Void) async {
        print("\n\(name)")
        await body(self)
    }

    private func record(pass name: String) {
        passed += 1
        print("  ok   \(name)")
    }

    private func record(failure name: String, _ error: Error) {
        failures.append("\(name): \(error)")
        print("  FAIL \(name) — \(error)")
    }

    /// Prints the tally and exits: 0 when everything passed, 1 otherwise.
    func finish() -> Never {
        print("\n\(passed) passed, \(failures.count) failed")
        for failure in failures { print("  - \(failure)") }
        exit(failures.isEmpty ? 0 : 1)
    }
}

struct Expectation: Error, CustomStringConvertible {
    let description: String
}

func expect(_ condition: Bool, _ message: String) throws {
    guard condition else { throw Expectation(description: message) }
}

/// Unwraps a value, failing the check with a readable message if it is absent.
///
/// - Parameters:
///   - value: the optional to unwrap.
///   - label: what was expected, used in the failure message.
func require<T>(_ value: T?, _ label: String) throws -> T {
    guard let value else { throw Expectation(description: "expected \(label), got nil") }
    return value
}

func expectEqual<T: Equatable>(_ actual: T, _ expected: T, _ label: String) throws {
    guard actual == expected else {
        throw Expectation(description: "\(label): expected \(expected), got \(actual)")
    }
}
