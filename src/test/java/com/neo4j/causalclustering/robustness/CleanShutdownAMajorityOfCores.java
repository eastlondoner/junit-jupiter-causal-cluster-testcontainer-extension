package com.neo4j.causalclustering.robustness;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.parallel.ExecutionMode.*;

import java.net.URI;
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
public class CleanShutdownAMajorityOfCores extends ClusterRobustnessTest.ClusterTestNoProxy {

	@CausalCluster
	static Collection<URI> clusterUri;

	@CausalCluster
	static Neo4jCluster cluster;

	@Override
	protected Collection<URI> getClusterUris() {
		return clusterUri;
	}

	@Test
	@Order(1)
	@Execution(SAME_THREAD)
	void t1StopSlowlyThenBringBackSimultaneously() throws InterruptedException, TimeoutException {
		// given
		Thread.sleep(5000);
		cluster.getAllServers().forEach(c -> c.getLatestLogs().contains("Considering restarting the actor system"));

		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
		checkAkkaState(clusterUri, 3);

		// when
		Set<Neo4jServer> stoppedServers = cluster.stopRandomServers(1);

		Collection<URI> runningServers = getClusterUrisExcluding(stoppedServers);
		assertThat(runningServers).hasSize(2);
		checkAkkaState(runningServers, 2);
		verifyRoutingTable(runningServers, 2, true);
		assertClusterUsable(runningServers, 2);

		// when another is stopped
		stoppedServers.addAll(cluster.stopRandomServersExcept(1, stoppedServers));
		runningServers = getClusterUrisExcluding(stoppedServers);

		// then
		verifyRoutingTable(clusterUri, 1, false);
		checkAkkaState(runningServers, 1);

		// when
		log.info(() -> "starting cores again");
		cluster.startServers(stoppedServers);

		// then
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
	}

	@Test
	@Order(2)
	@Execution(SAME_THREAD)
	void t2StopSimultaneouslyThenBringBackSimultaneously() throws InterruptedException, TimeoutException {
		// given
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);

		// when
		Set<Neo4jServer> stoppedServers = cluster.stopRandomServers(2);
		verifyServersStoppedGracefully(stoppedServers);

		// then
		verifyRoutingTable(clusterUri, 1, false);

		// when
		cluster.startServers(stoppedServers);

		// then
		Collection<URI> neverStoppedCore = getClusterUrisExcluding(stoppedServers);
		assertThat(neverStoppedCore).hasSize(1);
		verifyRoutingTable(neverStoppedCore, 3, true);
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
	}

	@Test
	@Order(3)
	@Execution(SAME_THREAD)
	void t3StopSimultaneouslyThenBringBackSlowly() throws InterruptedException, TimeoutException {
		// given
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);

		// when
		Set<Neo4jServer> stoppedServers = cluster.stopRandomServers(2);
		verifyServersStoppedGracefully(stoppedServers);

		// then
		verifyRoutingTable(clusterUri, 1, false);
		checkAkkaState(getClusterUrisExcluding(stoppedServers), 1);

		// when
		Set<Neo4jServer> coreToRestart = stoppedServers.stream().limit(1).collect(Collectors.toSet());
		stoppedServers.removeAll(coreToRestart);
		cluster.startServers(coreToRestart);

		// then
		verifyRoutingTable(clusterUri, 2, true);
		checkAkkaState(getClusterUrisExcluding(stoppedServers), 2);
		assertClusterUsable(clusterUri, 2);

		// finally return the cluster to a usable state
		cluster.startServers(stoppedServers);
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
		checkAkkaState(clusterUri, 3);
	}
}
