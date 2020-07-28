package com.neo4j.causalclustering.robustness;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.parallel.ExecutionMode.*;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jServer;

@Execution(CONCURRENT)
public class UncleanShutdownAMajorityOfCores extends ClusterRobustnessTest.ClusterTestNoProxy {

	@CausalCluster
	static Collection<URI> clusterUri;

	@CausalCluster
	static Neo4jCluster cluster;

	@Override
	protected Collection<URI> getClusterUris() {
		return clusterUri;
	}

	// TODO: akka treats the first listed seed node as special in certain circumstances. We should parameterise
	// so that we run each test with or without it in the surviving / failed set.

	@Test
	@Order(1)
	@Execution(SAME_THREAD)
	void t1KillSlowlyThenBringBackSimultaneously() throws InterruptedException, TimeoutException {
		// given
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
		checkAkkaState(clusterUri, 3);

		// when
		Set<Neo4jServer> killedServers = cluster.killRandomServers(1);
		Collection<URI> runningServers = getClusterUrisExcluding(killedServers);
		assertThat(runningServers).hasSize(2);

		verifyRoutingTable(runningServers, 2, true);
		assertClusterUsable(runningServers, 2);

		// It takes some time for the uncleanly shut down core to be removed by akka
		Thread.sleep(7500);
		checkAkkaState(runningServers, 2);

		killedServers.addAll(cluster.killRandomServersExcept(1, killedServers));

		// then
		verifyRoutingTable(clusterUri, 1, false);
		// TODO: assert that akka membership size is stable at 2 with 1 unreachable
		logAkkaState();

		// when
		cluster.startServers(killedServers);

		// then
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
		checkAkkaState(clusterUri, 3);
	}

	@Test
	@Order(2)
	@Execution(SAME_THREAD)
	void t2KillSlowlyThenBringBackInOrder() throws InterruptedException, TimeoutException {
		// given
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
		checkAkkaState(clusterUri, 3);

		// when
		Set<Neo4jServer> firstKilledCore = cluster.killRandomServers(1);
		Collection<URI> runningServers = getClusterUrisExcluding(firstKilledCore);
		assertThat(runningServers).hasSize(2);

		verifyRoutingTable(runningServers, 2, true);
		assertClusterUsable(runningServers, 2);

		// It takes some time for the uncleanly shut down core to be removed by akka
		Thread.sleep(7500);
		checkAkkaState(runningServers, 2);

		Set<Neo4jServer> secondKilledCore = cluster.killRandomServersExcept(1, firstKilledCore);

		// then
		verifyRoutingTable(clusterUri, 1, false);
		// TODO: assert that akka membership size is stable at 2 with 1 unreachable
		logAkkaState();

		// when
		cluster.startServers(firstKilledCore);
		runningServers = getClusterUrisExcluding(secondKilledCore);

		// then
		verifyRoutingTable(clusterUri, 2, true);
		assertClusterUsable(clusterUri, 2);
		checkAkkaState(runningServers, 2);

		// finally return the cluster to a full working state
		cluster.startServers(secondKilledCore);
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
		checkAkkaState(clusterUri, 3);
	}

	@Test
	@Order(3)
	@Execution(SAME_THREAD)
	void t3KillSlowlyThenBringBackInReverseOrder() throws InterruptedException, TimeoutException {
		// given
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
		checkAkkaState(clusterUri, 3);

		// when
		Set<Neo4jServer> firstKilledCore = cluster.killRandomServers(1);
		Collection<URI> runningServers = getClusterUrisExcluding(firstKilledCore);
		assertThat(runningServers).hasSize(2);

		verifyRoutingTable(runningServers, 2, true);
		assertClusterUsable(runningServers, 2);

		// It takes some time for the uncleanly shut down core to be removed by akka
		Thread.sleep(7500);
		checkAkkaState(runningServers, 2);

		Set<Neo4jServer> secondKilledCore = cluster.killRandomServersExcept(1, firstKilledCore);

		// then
		verifyRoutingTable(clusterUri, 1, false);
		// TODO: assert that akka membership size is stable at 2 with 1 unreachable
		logAkkaState();

		// when
		cluster.startServers(secondKilledCore);

		// then
		verifyRoutingTable(clusterUri, 2, true);
		assertClusterUsable(clusterUri, 2);
		checkAkkaState(runningServers, 2);

		// finally return the cluster to a full working state
		cluster.startServers(firstKilledCore);
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
		checkAkkaState(clusterUri, 3);
	}

	@Test
	@Order(4)
	@Execution(SAME_THREAD)
	void t4KillSimultaneouslyThenBringBackSimultaneously() throws InterruptedException, TimeoutException {
		// given
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
		checkAkkaState(clusterUri, 3);

		// when
		Set<Neo4jServer> killedServers = cluster.killRandomServers(2);

		// then
		verifyRoutingTable(clusterUri, 1, false);
		// TODO: assert that Akka membership is stable at 3 with 2 unreachable nodes here
		logAkkaState();

		// when
		cluster.startServers(killedServers);

		// then
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
		checkAkkaState(clusterUri, 3);
	}

	@Test
	@Order(5)
	@Execution(SAME_THREAD)
	void t5KillSimultaneouslyOnlyBringBackSlowly() throws InterruptedException, TimeoutException {
		// given
		verifyRoutingTable(clusterUri, 3, true, Duration.ofSeconds(30));
		assertClusterUsable(clusterUri, 3);

		// when
		Set<Neo4jServer> killedServers = cluster.killRandomServers(2);

		// then
		verifyRoutingTable(clusterUri, 1, false);
		logAkkaState();

		// when
		Set<Neo4jServer> restartedServers = cluster
			.startServers(killedServers.stream().limit(1).collect(Collectors.toSet()));
		killedServers.removeAll(restartedServers);
		// then
		verifyRoutingTable(clusterUri, 2, true);
		assertClusterUsable(clusterUri, 2);
		checkAkkaState(getClusterUrisExcluding(killedServers), 2);

		// Restart cores and get things in a functional state
		cluster.startServers(killedServers);
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
		checkAkkaState(clusterUri, 3);
	}
}
