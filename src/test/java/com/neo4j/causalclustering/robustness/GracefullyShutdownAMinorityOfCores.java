package com.neo4j.causalclustering.robustness;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.parallel.ExecutionMode.*;

import java.net.URI;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jServer;

@Execution(CONCURRENT)
public class GracefullyShutdownAMinorityOfCores extends ClusterRobustnessTest.ClusterTestNoProxy
{
	@CausalCluster
	static Collection<URI> clusterUri;

	@CausalCluster
	static Neo4jCluster cluster;

    private Set<Neo4jServer> coresWeShutdown;

    @BeforeEach
    private void before() throws InterruptedException, TimeoutException
    {
        assertNotNull(clusterUri);

        assertClusterUsable(clusterUri, 3);
        verifyRoutingTable(clusterUri, 3, true);
        checkAkkaState(clusterUri, 3);

        coresWeShutdown = cluster.stopRandomServers(1);
        verifyServersStoppedGracefully(coresWeShutdown);
        verifyRoutingTable(clusterUri, 2, true);
    }

    @Test
    @Order(1)
	@Execution(SAME_THREAD)
    void t1BringThemAllBack() throws InterruptedException, TimeoutException {
        // given
        assertClusterUsable(clusterUri, 2);

        // then
        cluster.startServers(coresWeShutdown);
        verifyRoutingTable(clusterUri, 3, true);
        assertClusterUsable(clusterUri, 3);
    }

    @Test
    @Order(2)
	@Execution(SAME_THREAD)
    void t2ReplaceWithANewCore() throws InterruptedException, TimeoutException {
        // given
        assertClusterUsable(clusterUri, 2);

        // then
        Assertions.fail( "Not yet implemented");
    }

	@Override
	protected Collection<URI> getClusterUris() {
		return clusterUri;
	}
}
