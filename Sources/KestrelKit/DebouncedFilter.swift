import Foundation
import Observation

/// Holds text as it is typed and publishes a query after typing pauses.
@MainActor
@Observable
public final class DebouncedFilter {
    public var text = "" {
        didSet {
            guard !isApplyingImmediately else { return }
            schedule()
        }
    }
    public private(set) var appliedText = ""

    @ObservationIgnored private var pending: Task<Void, Never>?
    @ObservationIgnored private var isApplyingImmediately = false
    @ObservationIgnored private let delay: Duration

    public init(delay: Duration = .milliseconds(250)) {
        self.delay = delay
    }

    /// Apply an external query or a clear action without waiting.
    public func setImmediately(_ value: String) {
        pending?.cancel()
        pending = nil
        isApplyingImmediately = true
        text = value
        isApplyingImmediately = false
        appliedText = value
    }

    public func clear() {
        setImmediately("")
    }

    private func schedule() {
        pending?.cancel()
        let value = text
        let delay = delay
        pending = Task { [weak self] in
            try? await Task.sleep(for: delay)
            guard !Task.isCancelled else { return }
            self?.appliedText = value
        }
    }
}
