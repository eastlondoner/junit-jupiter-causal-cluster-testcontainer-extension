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

import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.SocatContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.lifecycle.Startable;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.junit.jupiter.causal_cluster.ClusterFactory.DISCOVERY_PORT;
import static org.neo4j.junit.jupiter.causal_cluster.ClusterFactory.TOXIPROXY_INBOUND_NETWORK_ALIAS;
import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;

/**
 * @author Michael J. Simons
 */
final class DefaultCluster implements Neo4jCluster, CloseableResource {

	private final static String NEO4J_CONTAINER_START_MESSAGE = "Remote interface available at";

	private final SocatContainer boltProxy;
	private final List<DefaultNeo4jServer> clusterServers;
	private final Map<? extends Neo4jServer, ToxiproxyContainer> outboundToxiproxyContainers;
	private final Optional<ToxiproxyContainer> inboundToxiproxyContainer;

	DefaultCluster(SocatContainer boltProxy, Optional<ToxiproxyContainer> inboundToxiproxyContainer,
		Map<? extends Neo4jServer, ToxiproxyContainer> outboundToxiproxies, List<DefaultNeo4jServer> servers) {

		this.boltProxy = boltProxy;
		this.clusterServers = servers;
		this.inboundToxiproxyContainer = inboundToxiproxyContainer;
		this.outboundToxiproxyContainers = outboundToxiproxies;
	}

	@Override
	public URI getURI() {
		// Choose a random bolt port from the available ports
		DefaultNeo4jServer core = clusterServers.get(ThreadLocalRandom.current().nextInt(0, clusterServers.size()));
		return core.getURI();
	}

	@Override
	public Set<Neo4jServer> getAllServers() {
		return new HashSet<>(clusterServers);
	}

	@Override
	public Set<Neo4jServer> getAllServersExcept(Set<Neo4jServer> exclusions) {
		return clusterServers.stream().filter(c -> !exclusions.contains(c)).collect(Collectors.toSet());
	}

	//  -- Partition and Unpartition Servers --

	@Override
	public Set<Neo4jServer> partitionRandomServers(int n) {
		requireToxiproxyEnabled("partitionRandomServers");

		return randomServers(n).peek(core -> {

			setConnectionCut(core, DISCOVERY_PORT, true);
			setConnectionCut(core, 6000, true);
			setConnectionCut(core, 7000, true);

		}).collect(toSet());
	}

	@Override
	public Set<Neo4jServer> unpartitionServers(Set<Neo4jServer> Servers) {
		requireToxiproxyEnabled("unpartitionServers");

		return Servers.stream().peek(core -> {
			setConnectionCut(core, DISCOVERY_PORT, false);
			setConnectionCut(core, 6000, false);
			setConnectionCut(core, 7000, false);
		}).collect(toSet());
	}

	//  -- Stop, Kill and Start Servers --

	@Override
	public Set<Neo4jServer> stopRandomServers(int n) {
		return stopRandomServersExcept(n, Collections.emptySet());
	}

	private Neo4jContainer<?> unwrap(Neo4jServer server) {
		DefaultNeo4jServer impl = clusterServers.stream()
			.filter(server::equals)
			.findFirst()
			.orElseThrow(() -> new RuntimeException("Provided server does not exist in this cluster"));

		return impl.unwrap();
	}

	private String getClusterInternalHostname(Neo4jServer server) {
		List<String> aliases = unwrap(server).getNetworkAliases().stream()
			.filter(s -> s.startsWith("neo4j"))
			.collect(toList());

		if (aliases.size() == 0) {
			throw new RuntimeException("server has no network aliases");
		} else if (aliases.size() > 1) {
			throw new RuntimeException("server has too many network aliases");
		}

		return aliases.get(0);
	}

	@Override
	public Set<Neo4jServer> stopRandomServersExcept(int n, Set<Neo4jServer> exclusions) {
		List<Neo4jServer> chosenServers = chooseRandomServers(n, exclusions);

		return doToServers(chosenServers, server -> {
			int CORE_STOP_TIMEOUT_SECONDS = 120;
			Neo4jContainer<?> container = unwrap(server);

			WaitingConsumer consumer = new WaitingConsumer();
			container.followOutput(consumer, STDOUT);

			container.getDockerClient().stopContainerCmd(container.getContainerId())
				.withTimeout(CORE_STOP_TIMEOUT_SECONDS)
				.exec();

			try {
				consumer.waitUntil(frame -> frame.getUtf8String().contains("Stopped.\n"), CORE_STOP_TIMEOUT_SECONDS,
					TimeUnit.SECONDS);
				waitUntilContainerIsStopped(container);
			} catch (TimeoutException e) {
				throw new RuntimeException(e);
			}

			return server;
		}).collect(toSet());
	}

	@Override
	public Set<Neo4jServer> killRandomServers(int n) {
		return killRandomServersExcept(n, Collections.emptySet());
	}

	@Override
	public Set<Neo4jServer> killRandomServersExcept(int n, Set<Neo4jServer> exclusions) {
		List<Neo4jServer> chosenServers = chooseRandomServers(n, exclusions);

		return doToServers(chosenServers, server -> {
			Neo4jContainer<?> container = unwrap(server);
			container.getDockerClient().killContainerCmd(container.getContainerId()).exec();
			try {
				waitUntilContainerIsStopped(container);
			} catch (TimeoutException e) {
				throw new RuntimeException(e);
			}
			return server;
		}).collect(toSet());
	}

	@Override
	public Set<Neo4jServer> startServers(Set<Neo4jServer> servers) {
		return doToServers(servers, server -> {
			Neo4jContainer<?> container = unwrap(server);
			container.getDockerClient().startContainerCmd(container.getContainerId()).exec();

			/*
			The test container host ports are no longer valid after a restart so port based wait strategies don't work

			e.g. these both fail:
			WaitStrategy ws = new HttpWaitStrategy().forPort( 7474 ).forStatusCodeMatching( ( response) -> response == 200 );
			WaitStrategy ws = new HostPortWaitStrategy();
			 */

			WaitStrategy ws = new WaitForMessageInLatestLogs(NEO4J_CONTAINER_START_MESSAGE, server);
			ws.withStartupTimeout(Duration.ofMinutes(5)).waitUntilReady(container);

			return server;

		}).collect(toSet());
	}

	//  -- Pause and Unpause Servers --

	@Override
	public Set<Neo4jServer> pauseRandomServers(int n) {
		List<Neo4jServer> chosenServers = chooseRandomServers(n);

		return doToServers(chosenServers, server -> {
			Neo4jContainer<?> container = unwrap(server);
			container.getDockerClient().pauseContainerCmd(container.getContainerId()).exec();
			return server;
		}).collect(toSet());
	}

	@Override
	public Set<Neo4jServer> unpauseServers(Set<Neo4jServer> Servers) {

		return doToServers(Servers, server -> {
			Neo4jContainer<?> container = unwrap(server);
			container.getDockerClient().unpauseContainerCmd(container.getContainerId()).exec();
			return server;
		}).collect(toSet());
	}

	// -- Implement Closeable --

	@Override
	public void close() {

		boltProxy.close();
		clusterServers.forEach(DefaultNeo4jServer::close);
		inboundToxiproxyContainer.ifPresent(Startable::close);
		outboundToxiproxyContainers.forEach((k, v) -> v.close());
	}

	// -- Private Methods --

	private void waitUntilContainerIsStopped(Neo4jContainer<?> core) throws TimeoutException {
		Instant deadline = Instant.now().plus(Duration.ofSeconds(30));

		while (core.isRunning()) {
			if (Instant.now().isAfter(deadline)) {
				throw new TimeoutException("Timed out waiting for docker container to stop. " + core.getContainerId());
			}

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private Stream<Neo4jServer> randomServers(int n) {
		return randomServers(n, Collections.emptySet());
	}

	private Stream<Neo4jServer> randomServers(int n, Set<Neo4jServer> excluding) {

		List<Neo4jServer> candidates = clusterServers.stream().filter(c -> !excluding.contains(c)).collect(toList());
		int N = candidates.size();
		if (n > N) {
			throw new IllegalArgumentException("There are not enough valid members in the cluster.");
		} else if (n == N) {
			return candidates.stream();
		}
		Set<Neo4jServer> chosen = new HashSet<>(n);

		while (chosen.size() < n) {
			Neo4jServer chosenCore = candidates.get(ThreadLocalRandom.current().nextInt(0, N));
			if (!excluding.contains(chosenCore)) {
				chosen.add(chosenCore);
			}
		}

		return chosen.stream();
	}

	private List<Neo4jServer> chooseRandomServers(int n) {
		return randomServers(n).collect(toList());
	}

	private List<Neo4jServer> chooseRandomServers(int n, Set<Neo4jServer> excluding) {
		return randomServers(n, excluding).collect(toList());
	}

	private void setConnectionCut(Neo4jServer server, int port, boolean shouldCutConnection) {
		String hostname = getClusterInternalHostname(server);
		ToxiproxyContainer.ContainerProxy inboundProxy = this.inboundToxiproxyContainer.get().getProxy(hostname, port);
		ToxiproxyContainer.ContainerProxy outboundProxy = this.outboundToxiproxyContainers.get(server)
			.getProxy(TOXIPROXY_INBOUND_NETWORK_ALIAS, inboundProxy.getOriginalProxyPort());
		inboundProxy.setConnectionCut(shouldCutConnection);
		outboundProxy.setConnectionCut(shouldCutConnection);
	}

	private boolean isToxiproxyEnabled() {
		return inboundToxiproxyContainer.isPresent();
	}

	private void requireToxiproxyEnabled(String task) {
		if (!isToxiproxyEnabled()) {
			throw new IllegalStateException("Toxiproxy is required but not enabled for" + task
				+ ".\nSet `@NeedsCausalCluster(proxyInternalCommunication = true)` to enable this feature.");
		}
	}

	/**
	 * Performs an operation in parallel on the provided containers.
	 * This method does not return until the operation has completed on ALL containers.
	 *
	 * @param Servers Neo4jServers to operate on
	 * @param fn      operation to perform
	 * @param <T>     operation return type
	 * @return A Stream of the operation results
	 */
	private static <T> Stream<T> doToServers(Collection<Neo4jServer> Servers, Function<Neo4jServer, T> fn) {

		// TODO: better error handling
		final CountDownLatch latch = new CountDownLatch(Servers.size());
		List<CompletableFuture<T>> futures = Servers.stream()
			.map(randomCore -> CompletableFuture.supplyAsync(() -> {
				try {
					return fn.apply(randomCore);
				} finally {
					latch.countDown();
				}
			})).collect(toList());

		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		return futures.stream().map(CompletableFuture::join);
	}
}
