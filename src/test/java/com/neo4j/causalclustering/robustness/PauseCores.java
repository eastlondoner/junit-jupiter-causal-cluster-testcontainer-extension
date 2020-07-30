package com.neo4j.causalclustering.robustness;

import static org.junit.jupiter.api.parallel.ExecutionMode.*;

import java.net.URI;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jServer;

@Execution(CONCURRENT)
public class PauseCores extends ClusterRobustnessTest.ClusterTestWithProxy {

	@CausalCluster
	static Collection<URI> clusterUri;

	@CausalCluster
	static Neo4jCluster cluster;

	@Override
	protected Collection<URI> getClusterUris() {
		return clusterUri;
	}

	@Test
	@Execution(SAME_THREAD)
	void pauseOneAndRestartTheOthers() throws InterruptedException, TimeoutException {

		assertClusterUsable(clusterUri, 3);
		verifyRoutingTable(clusterUri, 3, true);

		Set<Neo4jServer> pausedCore = cluster.pauseRandomServers(1);

		verifyRoutingTable(clusterUri, 2, true);
		assertClusterUsable(clusterUri, 2);

		Set<Neo4jServer> killedServers = cluster.killRandomServersExcept(2, pausedCore);
		cluster.startServers(killedServers);
		verifyRoutingTable(clusterUri, 2, true);
		assertClusterUsable(clusterUri, 2);

		cluster.unpauseServers(pausedCore);

		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
	}
}
