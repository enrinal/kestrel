import KestrelKit
import SwiftUI

struct SidebarView: View {
    @Environment(ClusterStore.self) private var store

    var body: some View {
        List(selection: Binding(get: { store.selection }, set: { store.selection = $0 })) {
            Section("Clusters") {
                ForEach(store.clusters) { cluster in
                    ClusterTree(cluster: cluster)
                }
            }
        }
        .listStyle(.sidebar)
        .navigationSplitViewColumnWidth(min: 220, ideal: 260)
        .overlay {
            if store.clusters.isEmpty {
                ContentUnavailableView {
                    Label("No Clusters", systemImage: "server.rack")
                } description: {
                    Text("Add a Kafka cluster to start exploring.")
                }
            }
        }
        .safeAreaInset(edge: .top) { filterField }
        .safeAreaInset(edge: .bottom) {
            Button("Add Cluster", systemImage: "plus") { store.beginAdd() }
                .buttonStyle(.borderless)
                .padding(8)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    /// Narrows the topic and consumer group branches.
    ///
    /// Its own field rather than `.searchable`: this filters the tree in
    /// place, while Find Messages goes back to the broker, and two search
    /// affordances that read alike but do different things is worse than one
    /// plain field that says what it narrows.
    private var filterField: some View {
        let filter = Binding(get: { store.sidebarFilter }, set: { store.sidebarFilter = $0 })

        return HStack(spacing: 6) {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.secondary)
            TextField("Filter topics and groups", text: filter)
                .textFieldStyle(.plain)
            if !store.sidebarFilter.isEmpty {
                Button {
                    store.sidebarFilter = ""
                } label: {
                    Image(systemName: "xmark.circle.fill")
                }
                .buttonStyle(.plain)
                .foregroundStyle(.secondary)
                .help("Clear the filter")
            }
        }
        .padding(.horizontal, 6)
        .padding(.vertical, 4)
        .background(.quaternary, in: RoundedRectangle(cornerRadius: 6))
        .padding(.horizontal, 10)
        .padding(.bottom, 6)
    }
}

/// One cluster and, once connected, its Brokers / Topics / Consumer Groups
/// branches.
private struct ClusterTree: View {
    let cluster: ClusterProfile

    @Environment(ClusterStore.self) private var store

    private var connection: ClusterConnection? { store.connection(for: cluster.id) }

    private func expansion(_ item: SidebarItem) -> Binding<Bool> {
        Binding(get: { store.isExpanded(item) }, set: { store.setExpanded(item, $0) })
    }

    var body: some View {
        let isExpanded = expansion(.cluster(cluster.id))

        DisclosureGroup(isExpanded: isExpanded) {
            switch connection?.phase {
            case .connected:
                branches
            case .connecting:
                Label { Text("Connecting…") } icon: { ProgressView().controlSize(.small) }
            case .failed(let message):
                Label(message, systemImage: "exclamationmark.triangle.fill")
                    .foregroundStyle(.secondary)
                    .lineLimit(3)
            case .idle, nil:
                Button("Connect") { Task { await store.connect(id: cluster.id) } }
                    .buttonStyle(.link)
            }
        } label: {
            Label(cluster.name, systemImage: "server.rack")
                .tag(SidebarItem.cluster(cluster.id))
                .contextMenu { menu }
        }
        // Expanding a disconnected cluster connects it, which is what a user
        // means by clicking the triangle.
        .onChange(of: isExpanded.wrappedValue) {
            guard isExpanded.wrappedValue, connection == nil else { return }
            Task { await store.connect(id: cluster.id) }
        }
    }

    @ViewBuilder
    private var branches: some View {
        let brokers = connection?.brokers ?? []
        let topics = store.visibleTopics(for: cluster.id)

        DisclosureGroup(isExpanded: expansion(.brokersFolder(cluster.id))) {
            ForEach(brokers) { broker in
                Label("\(broker.id) · \(broker.endpoint)", systemImage: "cpu")
                    .tag(SidebarItem.broker(cluster.id, broker.id))
            }
        } label: {
            Label("Brokers (\(brokers.count))", systemImage: "rectangle.stack")
                .tag(SidebarItem.brokersFolder(cluster.id))
        }

        DisclosureGroup(isExpanded: expansion(.topicsFolder(cluster.id))) {
            ForEach(topics.prefix(ClusterStore.sidebarRowLimit)) { topic in
                Label(topic.name, systemImage: topic.isInternal ? "lock.rectangle" : "tray.full")
                    .tag(SidebarItem.topic(cluster.id, topic.name))
            }
            overflowRow(shown: ClusterStore.sidebarRowLimit, of: topics.count)
        } label: {
            Label(countLabel("Topics", shown: topics.count, of: totalTopics), systemImage: "tray.2")
                .tag(SidebarItem.topicsFolder(cluster.id))
        }

        let showsGroups = expansion(.groupsFolder(cluster.id))

        DisclosureGroup(isExpanded: showsGroups) {
            switch connection?.groupsLoad {
            case .loading:
                Label { Text("Loading…") } icon: { ProgressView().controlSize(.small) }
            case .failed(let message):
                Label(message, systemImage: "exclamationmark.triangle.fill")
                    .foregroundStyle(.secondary)
                    .lineLimit(3)
            default:
                let groups = store.visibleGroups(for: cluster.id)
                ForEach(groups.prefix(ClusterStore.sidebarRowLimit)) { group in
                    Label(group.id, systemImage: "person.3")
                        .tag(SidebarItem.group(cluster.id, group.id))
                }
                overflowRow(shown: ClusterStore.sidebarRowLimit, of: groups.count)
            }
        } label: {
            Label(groupsLabel, systemImage: "person.2.badge.gearshape")
                .tag(SidebarItem.groupsFolder(cluster.id))
        }
        // Consumer groups need their own request, so they load on first look.
        .onChange(of: showsGroups.wrappedValue) {
            guard showsGroups.wrappedValue else { return }
            Task { await connection?.loadGroups() }
        }

        // Only for clusters that have a worker configured. The branch stays put
        // once it is there, even when the worker is down, so a failure reads as
        // a failure rather than as a lost setting.
        if connection?.hasConnect == true {
            let showsConnectors = expansion(.connectFolder(cluster.id))

            DisclosureGroup(isExpanded: showsConnectors) {
                switch connection?.connectorsLoad {
                case .loading:
                    Label { Text("Loading…") } icon: { ProgressView().controlSize(.small) }
                case .failed(let message):
                    Label(message, systemImage: "exclamationmark.triangle.fill")
                        .foregroundStyle(.secondary)
                        .lineLimit(3)
                default:
                    ForEach(connection?.connectors ?? []) { connector in
                        Label(
                            connector.name,
                            systemImage: connector.needsAttention
                                ? "exclamationmark.triangle"
                                : (connector.kind == .sink ? "tray.and.arrow.down" : "tray.and.arrow.up")
                        )
                        .tag(SidebarItem.connector(cluster.id, connector.name))
                    }
                }
            } label: {
                Label(connectLabel, systemImage: "point.3.connected.trianglepath.dotted")
                    .tag(SidebarItem.connectFolder(cluster.id))
            }
            .onChange(of: showsConnectors.wrappedValue) {
                guard showsConnectors.wrappedValue else { return }
                Task { await connection?.loadConnectors() }
            }
        }
    }

    private var connectLabel: String {
        guard let connection, connection.connectorsLoad == .loaded else { return "Connect" }
        return "Connect (\(connection.connectors.count))"
    }

    private var groupsLabel: String {
        guard let connection, connection.groupsLoad == .loaded else { return "Consumer Groups" }
        return countLabel(
            "Consumer Groups",
            shown: store.visibleGroups(for: cluster.id).count,
            of: connection.groups.count
        )
    }

    /// Topics on the cluster before the filter, for the "23 of 1,819" label.
    private var totalTopics: Int {
        guard let connection else { return 0 }
        return store.showsInternalTopics
            ? connection.topics.count
            : connection.topics.count { !$0.isInternal }
    }

    /// `Topics (1,819)`, or `Topics (23 of 1,819)` while the filter is on.
    ///
    /// Both numbers, because a branch that only showed the filtered count
    /// would read as a cluster that had lost its topics.
    private func countLabel(_ title: String, shown: Int, of total: Int) -> String {
        shown == total
            ? "\(title) (\(total.formatted()))"
            : "\(title) (\(shown.formatted()) of \(total.formatted()))"
    }

    /// The row that stands in for what `sidebarRowLimit` cut off.
    @ViewBuilder
    private func overflowRow(shown: Int, of total: Int) -> some View {
        if total > shown {
            Label(
                "\((total - shown).formatted()) more — filter to narrow",
                systemImage: "ellipsis.circle"
            )
            .foregroundStyle(.secondary)
        }
    }

    @ViewBuilder
    private var menu: some View {
        if connection?.isConnected == true {
            Button("Refresh") { Task { await store.refresh(id: cluster.id) } }
            Button("Disconnect") { store.disconnect(id: cluster.id) }
        } else {
            Button("Connect") { Task { await store.connect(id: cluster.id) } }
        }
        Divider()
        Button("Edit…") { store.beginEdit(id: cluster.id) }
        Button("Remove", role: .destructive) { store.remove(id: cluster.id) }
    }
}
