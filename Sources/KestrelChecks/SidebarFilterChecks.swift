import Foundation
import KestrelKit

@MainActor
func registerSidebarFilterChecks(_ harness: Harness) async {
    await harness.asyncSuite("Sidebar filter") { h in
        await h.checkAsync("typing waits and applies only the latest query") {
            let filter = DebouncedFilter()
            filter.text = "ord"
            try await Task.sleep(for: .milliseconds(100))
            filter.text = "orders"
            try expectEqual(filter.appliedText, "", "query before the pause")
            try await Task.sleep(for: .milliseconds(180))
            try expectEqual(filter.appliedText, "", "cancelled query")
            try await Task.sleep(for: .milliseconds(120))
            try expectEqual(filter.appliedText, "orders", "latest query")
        }

        await h.checkAsync("clear cancels a pending query immediately") {
            let filter = DebouncedFilter()
            filter.setImmediately("orders")
            filter.text = "payments"
            filter.clear()
            try expectEqual(filter.text, "", "field after clear")
            try expectEqual(filter.appliedText, "", "query after clear")
            try await Task.sleep(for: .milliseconds(300))
            try expectEqual(filter.appliedText, "", "query after cancelled wait")
        }

        await h.checkAsync("an external query replaces the field and pending change") {
            let filter = DebouncedFilter()
            filter.text = "pending"
            filter.setImmediately("snapshot")
            try expectEqual(filter.text, "snapshot", "field after external change")
            try expectEqual(filter.appliedText, "snapshot", "applied external query")
            try await Task.sleep(for: .milliseconds(300))
            try expectEqual(filter.appliedText, "snapshot", "query after cancelled wait")
        }
    }
}
