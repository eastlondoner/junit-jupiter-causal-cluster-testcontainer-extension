package com.neo4j.causalclustering.robustness;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.parallel.ExecutionMode.*;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
public class PartitionCores extends ClusterRobustnessTest.ClusterTestWithProxy {

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
	void t1PartitionSingleCore() throws InterruptedException, TimeoutException {

		assertClusterUsable(clusterUri, 3);
		verifyRoutingTable(clusterUri, 3, true);

		Set<Neo4jServer> cores = cluster.partitionRandomServers(1);
		assertThat(cores).hasSize(1);

		Set<URI> partitionedCoreUris = cores.stream().map(Neo4jServer::getURI).collect(Collectors.toSet());

		List<URI> otherCoreUris = clusterUri.stream().filter(uri -> !partitionedCoreUris.contains(uri))
			.collect(Collectors.toList());
		log.info(() -> "Partitioned cores: " + partitionedCoreUris.stream().map(URI::toString)
			.collect(Collectors.joining(",")));
		assertThat(otherCoreUris).hasSize(2);

		verifyRoutingTable(partitionedCoreUris, 1, false);
		verifyRoutingTable(otherCoreUris, 2, true);
		assertClusterUsable(otherCoreUris, 2);

		cluster.unpartitionServers(cores);
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
	}

	@Test
	@Order(2)
	@Execution(SAME_THREAD)
	void t2PartitionEveryoneFromEveryoneElse() throws InterruptedException, TimeoutException {

		assertClusterUsable(clusterUri, 3);
		verifyRoutingTable(clusterUri, 3, true);

		Set<Neo4jServer> cores = cluster.partitionRandomServers(3);
		assertThat(cores).hasSize(3);

		Set<URI> partitionedCoreUris = cores.stream().map(Neo4jServer::getURI).collect(Collectors.toSet());

		for (URI uri : partitionedCoreUris) {
			verifyRoutingTable(Collections.singletonList(uri), 1, false);
		}

		cluster.unpartitionServers(cores);
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
	}

	@Test
	@Order(3)
	@Execution(SAME_THREAD)
	void t3PartitionOneAndRestartTheOthers() throws InterruptedException, TimeoutException {

		assertClusterUsable(clusterUri, 3);
		verifyRoutingTable(clusterUri, 3, true);

		Set<Neo4jServer> partitionedCore = cluster.partitionRandomServers(1);
		Set<URI> partitionedCoreUris = partitionedCore.stream().map(Neo4jServer::getURI)
			.collect(Collectors.toSet());
		List<URI> otherCoreUris = clusterUri.stream().filter(uri -> !partitionedCoreUris.contains(uri))
			.collect(Collectors.toList());
		assertThat(otherCoreUris).hasSize(2);

		verifyRoutingTable(partitionedCoreUris, 1, false);
		verifyRoutingTable(otherCoreUris, 2, true);
		assertClusterUsable(otherCoreUris, 2);

        /*
        Set<Neo4jServer> killedServers = cluster.stopRandomServersExcept(1, partitionedCore);
        cluster.startServers(killedServers);
        verifyRoutingTable(otherCoreUris, 2, true);
        assertClusterUsable(otherCoreUris, 2);
        */
		// TODO: figure out why this sleep is needed
		Thread.sleep(15 * 1000);
		verifyRoutingTable(otherCoreUris, 2, true);
		assertClusterUsable(otherCoreUris, 2);

		Set<Neo4jServer> killedServers = cluster.killRandomServersExcept(2, partitionedCore);
		cluster.startServers(killedServers);
		verifyRoutingTable(otherCoreUris, 2, true);
		assertClusterUsable(otherCoreUris, 2);

		cluster.unpartitionServers(partitionedCore);
		verifyRoutingTable(clusterUri, 3, true);
		assertClusterUsable(clusterUri, 3);
	}

}
