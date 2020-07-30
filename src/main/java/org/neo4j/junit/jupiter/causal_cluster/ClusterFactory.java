/*
 * Copyright (c) 2019-2020 "Neo4j,"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.junit.jupiter.causal_cluster;

import static java.util.stream.Collectors.*;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.SocatContainer;
import org.testcontainers.containers.ToxiproxyContainer;

/**
 * Creates new cluster instances.
 *
 * @author Michael J. Simons
 * @author Andrew Jefferson
 */
final class ClusterFactory {

	private static final int DEFAULT_BOLT_PORT = 7687;
	private static final int DEFAULT_HTTP_PORT = 7474;

	static final int DISCOVERY_PORT = 5000;

	// An alias that can be used to resolve the Toxiproxy container by name in the network it is connected to.
	// It can be used as a hostname of the Toxiproxy container by other containers in the same network.
	static final String TOXIPROXY_INBOUND_NETWORK_ALIAS = "inboundtoxiproxy";
	private static final String TOXIPROXY_OUTBOUND_NETWORK_ALIAS = "outboundtoxiproxy";

	private final Configuration configuration;

	private SocatContainer boltProxy;
	private Optional<ToxiproxyContainer> inboundToxiproxy;
	private List<ToxiproxyContainer> outboundToxiproxies;

	ClusterFactory(Configuration configuration) {
		this.configuration = configuration;
	}

	Neo4jCluster createCluster() {

		final int numberOfCoreMembers = configuration.getNumberOfCoreMembers();
		final Map<Integer, String> configuredCores = configuration.iterateCoreMembers()
			.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		// Prepare one shared network for those containers
		final Network network = Network.newNetwork();

		// Start network proxies
		startBoltProxy(configuredCores, network);
		startClusterProxiesIfNecessary(numberOfCoreMembers, network);

		// Build the neo4j TestContainer Containers
		Map<String, Neo4jContainer<?>> cluster = configuredCores.entrySet().stream()
			.collect(toMap(
				Map.Entry::getValue,
				entry -> buildNeo4jContainer(network, entry)
			));

		// Now figure out what the initial discovery members are and add that to the Neo4jContainers
		cluster.replaceAll((c, v) -> v.withNeo4jConfig(
			"causal_clustering.initial_discovery_members", getInitialDiscoveryMembers(configuredCores)
		));

		// Start all the Neo4j Cores in parallel
		final CountDownLatch latch = new CountDownLatch(numberOfCoreMembers);
		cluster.values().forEach(instance -> CompletableFuture.runAsync(() -> {
			// TODO: error handling if something goes wrong in here.
			instance.start();
			latch.countDown();
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		// Construct the Neo4jCore and CausalCluster objects
		Map<Neo4jServer, ToxiproxyContainer> outboundToxiproxyMap = new HashMap<>(configuredCores.size());
		List<DefaultNeo4jServer> cores = new ArrayList<>(configuredCores.size());
		for (Map.Entry<Integer, String> entry : configuredCores.entrySet()) {
			int coreIndex = entry.getKey();
			String hostname = entry.getValue();

			URI neo4jUri = buildNeo4jUri(boltProxy, DEFAULT_BOLT_PORT + coreIndex);
			ToxiproxyContainer outboundToxyiproxy = configuration.isToxiproxyEnabled() ?
				outboundToxiproxies.get(coreIndex)
				: null;

			DefaultNeo4jServer server = new DefaultNeo4jServer(cluster.get(hostname), neo4jUri, outboundToxyiproxy);
			outboundToxiproxyMap.put(server, outboundToxyiproxy);
			cores.add(server);
		}

		return new DefaultCluster(boltProxy, inboundToxiproxy, outboundToxiproxyMap, cores);
	}

	private String getInitialDiscoveryMembers(Map<Integer, String> configuredCores) {
		return configuredCores.entrySet().stream()
			.map(entry -> {
				int coreIndex = entry.getKey();
				String hostname = entry.getValue();

				return configuration.isToxiproxyEnabled() ?
					getProxiedAddressFor(inboundToxiproxy.get(), outboundToxiproxies.get(coreIndex), hostname)
					: new HostnameAndPort(hostname, DISCOVERY_PORT);
			})
			.map(HostnameAndPort::toString)
			.collect(joining(","));
	}

	private void startClusterProxiesIfNecessary(int numberOfCoreMembers, Network network) {
		// We will use Toxiproxy to allow us to inject failures in communication between neo4j cores.
		// The setup here is a bit complicated because Neo4j Cores communicate with one another as both clients and servers.
		// To simulate a network split we have to inject failure into both server (inbound) and client (outbound) communication with a core.
		// Managing server (inbound) connections to a core is easily done with a single shared Toxiproxy container which has
		// a (server) proxy for each core.
		// Managing client (outbound) connections is tricky and requires that we create a Toxiproxy container for each core which is used to
		// proxy *all* outgoing connections from that core.
		// The result is that cluster communications are routed like so:
		// (CORE {id:A})-[]->(OUTBOUND_TOXIPROXY_A {shared:false})->[]->(INBOUND_TOXIPROXY {proxy_for:A, shared:true})->(CORE {id:B})
		// (CORE {id:B})-[]->(OUTBOUND_TOXIPROXY_B {shared:false})->[]->(INBOUND_TOXIPROXY {proxy_for:B, shared:true})->(CORE {id:A})
		// To simulate a network split on a single core we have to close down all client connections (we do this
		inboundToxiproxy = Optional
			.ofNullable(configuration.isToxiproxyEnabled() ? new ToxiproxyContainer()
				.withNetwork(network)
				.withNetworkAliases(TOXIPROXY_INBOUND_NETWORK_ALIAS) : null);

		// start toxiproxy so we can request proxy ports
		inboundToxiproxy.ifPresent(GenericContainer::start);

		outboundToxiproxies = configuration.isToxiproxyEnabled() ?
			IntStream.rangeClosed(1, numberOfCoreMembers)
				.mapToObj(ignored -> new ToxiproxyContainer().withNetwork(network))
				.collect(toList())
			: Collections.emptyList();

		outboundToxiproxies.forEach(GenericContainer::start);
	}

	private void startBoltProxy(Map<Integer, String> configuredCores, Network network) {
		// boltProxy is used for routing bolt (cypher) connections from drivers in test code to cores in the cluster
		// Prepare boltProxy to enter the cluster
		boltProxy = new SocatContainer().withNetwork(network);

		configuredCores.forEach(
			(key, value) -> boltProxy.withTarget(DEFAULT_BOLT_PORT + key, value, DEFAULT_BOLT_PORT));
		configuredCores.forEach(
			(key, value) -> boltProxy.withTarget(DEFAULT_HTTP_PORT + key, value, DEFAULT_HTTP_PORT));

		// Start the boltProxy so that the exposed ports are available and we can get the mapped ones
		boltProxy.start();
	}

	private Neo4jContainer<?> buildNeo4jContainer(Network network, Map.Entry<Integer, String> member) {

		final int numberOfCoreMembers = configuration.getNumberOfCoreMembers();
		String neo4jVersion = configuration.getNeo4jVersion();
		final boolean is35 = neo4jVersion.startsWith("3.5");
		String neo4jContainerHostname = member.getValue();

		Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(configuration.getImageName())
			.withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
			.withAdminPassword(configuration.getPassword())
			.withNetwork(network)
			.withNetworkAliases(neo4jContainerHostname)
			.withCreateContainerCmdModifier(cmd -> cmd.withHostName(neo4jContainerHostname))
			.withNeo4jConfig("dbms.mode", "CORE")
			.withNeo4jConfig("dbms.memory.pagecache.size", configuration.getPagecacheSize() + "M")
			.withNeo4jConfig("dbms.memory.heap.initial_size", configuration.getInitialHeapSize() + "M")
			.withNeo4jConfig(is35 ? "dbms.connectors.default_listen_address" : "dbms.default_listen_address",
				"0.0.0.0")
			.withNeo4jConfig("dbms.connector.bolt.advertised_address", String
				.format("%s:%d", boltProxy.getContainerIpAddress(),
					boltProxy.getMappedPort(DEFAULT_BOLT_PORT + member.getKey())))
			.withNeo4jConfig("dbms.connector.http.advertised_address", String
				.format("%s:%d", boltProxy.getContainerIpAddress(),
					boltProxy.getMappedPort(DEFAULT_HTTP_PORT + member.getKey())))
			.withNeo4jConfig("causal_clustering.minimum_core_cluster_size_at_formation",
				Integer.toString(numberOfCoreMembers))
			.withNeo4jConfig("causal_clustering.minimum_core_cluster_size_at_runtime",
				Integer.toString(numberOfCoreMembers))
			.withNeo4jConfig("causal_clustering.middleware.logging.level", "DEBUG")
			.withNeo4jConfig("dbms.logs.debug.level", "DEBUG")
			.withStartupTimeout(configuration.getStartupTimeout());

		if (!configuration.getNeo4jSourceOverride().isEmpty()) {
			String neo4jSource = configuration.getNeo4jSourceOverride();
			String packagingOutput = "private/packaging/standalone/target/neo4j-enterprise-<neo4j_version>-SNAPSHOT-unix/neo4j-enterprise-<neo4j_version>-SNAPSHOT"
				.replaceAll("<neo4j_version>", neo4jVersion);
			Path packagingDir = Paths.get(neo4jSource).resolve(packagingOutput);

			if (!Files.exists(packagingDir)) {
				throw new IllegalStateException(
					"Provided neo4j source directory does not contain packaged output at: " + packagingOutput);
			}
			neo4jContainer = neo4jContainer
				.withFileSystemBind(packagingDir.resolve("lib").toAbsolutePath().toString(), "/var/lib/neo4j/lib/");
			neo4jContainer = neo4jContainer
				.withFileSystemBind(packagingDir.resolve("bin").toAbsolutePath().toString(), "/var/lib/neo4j/bin/");
		}
		if (configuration.isToxiproxyEnabled()) {
			assert inboundToxiproxy.isPresent();
			HostnameAndPort discoveryAddress = proxyContainerPort(inboundToxiproxy.get(), outboundToxiproxies,
				neo4jContainerHostname, DISCOVERY_PORT);
			HostnameAndPort transactionAddress = proxyContainerPort(inboundToxiproxy.get(), outboundToxiproxies,
				neo4jContainerHostname, 6000);
			HostnameAndPort raftAddress = proxyContainerPort(inboundToxiproxy.get(), outboundToxiproxies,
				neo4jContainerHostname,
				7000);

			ToxiproxyContainer myOutboundContainer = outboundToxiproxies.get(member.getKey());

			neo4jContainer = neo4jContainer.withNeo4jConfig(
				is35 ? "dbms.connectors.default_advertised_address" : "dbms.default_advertised_address",
				TOXIPROXY_OUTBOUND_NETWORK_ALIAS)
				.withNeo4jConfig("causal_clustering.discovery_advertised_address", discoveryAddress.toString())
				.withNeo4jConfig("causal_clustering.transaction_advertised_address", transactionAddress.toString())
				.withNeo4jConfig("causal_clustering.raft_advertised_address", raftAddress.toString())
				.withExtraHost(TOXIPROXY_OUTBOUND_NETWORK_ALIAS,
					myOutboundContainer.getCurrentContainerInfo()
						.getNetworkSettings()
						.getNetworks()
						.get(((Network.NetworkImpl) network).getName())
						.getIpAddress()
				);

		} else {
			neo4jContainer = neo4jContainer.withNeo4jConfig(
				is35 ? "dbms.connectors.default_advertised_address" : "dbms.default_advertised_address",
				neo4jContainerHostname);
		}

		return neo4jContainer;
	}

	private HostnameAndPort proxyContainerPort(
		ToxiproxyContainer inboundProxy,
		List<ToxiproxyContainer> outboundProxy,
		String neo4jContainerHostname,
		int port
	) {
		int inboundProxyPort = inboundProxy.getProxy(neo4jContainerHostname, port).getOriginalProxyPort();
		HostnameAndPort destination = new HostnameAndPort(TOXIPROXY_OUTBOUND_NETWORK_ALIAS, inboundProxyPort);

		// We have to call getProxy on all the outbound proxies for the proxy to be created.
		List<ToxiproxyContainer.ContainerProxy> outboundProxies = outboundProxy.stream()
			.map(p -> p.getProxy(TOXIPROXY_INBOUND_NETWORK_ALIAS, inboundProxyPort))
			.collect(toList());

		// We rely on the fact that, since we do the same operations in the same order, all the outbound proxies
		// will assign the same ports to the same destinations.	We check that here.
		List<Integer> outboundProxyPorts = outboundProxies.stream()
			.map(ToxiproxyContainer.ContainerProxy::getOriginalProxyPort)
			.distinct()
			.collect(toList());
		if (outboundProxyPorts.size() != 1 || inboundProxyPort != outboundProxyPorts.get(0)) {
			throw new IllegalStateException("all inbound and outbound proxy port numbers must match");
		}
		return destination;
	}

	private HostnameAndPort getProxiedAddressFor(ToxiproxyContainer inboundProxy,
		ToxiproxyContainer outboundProxy, String hostname) {
		int inboundPort = inboundProxy.getProxy(hostname, DISCOVERY_PORT).getOriginalProxyPort();
		int outboundPort = outboundProxy.getProxy(TOXIPROXY_INBOUND_NETWORK_ALIAS, inboundPort).getOriginalProxyPort();
		return new HostnameAndPort(
			TOXIPROXY_OUTBOUND_NETWORK_ALIAS,
			outboundPort
		);
	}

	private static URI buildNeo4jUri(SocatContainer proxy, int port) {
		return URI.create(String.format("neo4j://%s:%d", proxy.getContainerIpAddress(), proxy.getMappedPort(port)));
	}
}
