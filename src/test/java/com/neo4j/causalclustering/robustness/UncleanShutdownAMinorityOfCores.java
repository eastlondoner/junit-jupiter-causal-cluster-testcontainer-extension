package com.neo4j.causalclustering.robustness;

import static org.junit.jupiter.api.Assertions.*;
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
public class UncleanShutdownAMinorityOfCores extends ClusterRobustnessTest.ClusterTestNoProxy {

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
	void bringThemAllBack() throws InterruptedException, TimeoutException {

		assertNotNull(clusterUri);
		assertClusterUsable(clusterUri, 3);
		verifyRoutingTable(clusterUri, 3, true);
		Set<Neo4jServer> coreIds = cluster.killRandomServers(1);
		verifyRoutingTable(clusterUri, 2, true);
		assertClusterUsable(clusterUri, 2);
		verifyServersDidNotStopGracefully(coreIds);
		cluster.startServers(coreIds);
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
	}
}
