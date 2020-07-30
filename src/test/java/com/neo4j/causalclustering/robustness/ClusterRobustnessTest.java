package com.neo4j.causalclustering.robustness;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.parallel.ExecutionMode.*;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.junit.jupiter.causal_cluster.NeedsCausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jServer;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Execution(CONCURRENT)
public class ClusterRobustnessTest {
	private static final Duration CORE_REJOIN_TIMEOUT = Duration.ofMinutes(6);
	private static final Duration DB_ONLINE_TIMEOUT = Duration.ofMinutes(1);
	private static final String GRACEFUL_SHUTDOWN_SUCCESS_LOG = "Stopped.\n";
	private static final int TRANSACTION_RETRY_INTERVAL_MILLISECONDS = 5000;
	private static final TransactionWork<Map<String, Value>> getAkkaState = tx -> {
		Result result = tx.run(
			"CALL dbms.queryJmx(\"akka:*\") YIELD attributes UNWIND [ k in keys(attributes) | [k,attributes[k].value] ] as val WITH val WHERE val[1] is not NULL RETURN val[0] as key, val[1] as value");
		Map<String, Value> records = result.stream()
			.collect(Collectors.toMap(r -> r.get("key").asString(), r -> r.get("value")));
		assertThat(records.keySet())
			.containsOnly("Leader", "Unreachable", "Singleton", "Available", "MemberStatus", "Members",
				"ClusterStatus");

		return records;
	};

	private static void checkDummyWriteTx(Session session) {
		List<Record> results = session.writeTransaction((tx) -> {
			Result result = tx.run("RETURN 1 AS foo");
			return result.list();
		});
		assertThat(results).hasSize(1);
		assertThat(results.get(0).get("foo").asInt()).isEqualTo(1);
	}

	private static void checkDummyReadTx(Session session) {
		List<Record> results = session.readTransaction((tx) -> {
			Result result = tx.run("RETURN 1 AS foo");
			return result.list();
		});
		assertThat(results).hasSize(1);
		assertThat(results.get(0).get("foo").asInt()).isEqualTo(1);
	}

	private static String checkShowDatabases(Session session, int expectedSize, boolean expectedWritable) {
		List<Record> results = session.readTransaction((tx) -> {
			Result result = tx.run("SHOW DATABASES");
			return result.list();
		});
		assertThat(results).hasSize(expectedSize);
		assertThat(results.stream().map(row -> row.get("currentStatus").asString()).distinct()).containsOnly("online");
		if (expectedWritable) {
			assertThat(
				results.stream().map(row -> row.get("role").asString()).filter(r -> r.equalsIgnoreCase("leader")))
				.hasSize(2);
		}

		return results.stream().flatMap(r -> r.asMap(Values.ofValue()).entrySet().stream())
			.map(k -> k.getKey() + ": " + k.getValue()).collect(
				Collectors.joining("\n"));
	}

	@NeedsCausalCluster(
		customImageName = "neo4j:4.1-enterprise",
		overrideWithLocalNeo4jSource = "/Users/andrew/neo4j/repos/neo4j/4.1",
		neo4jVersion = "4.1.2"
	)
	public static abstract class ClusterTestNoProxy extends ClusterTestBase {
	}

	@NeedsCausalCluster(
		customImageName = "neo4j:4.1-enterprise",
		overrideWithLocalNeo4jSource = "/Users/andrew/neo4j/repos/neo4j/4.1",
		neo4jVersion = "4.1.2",
		proxyInternalCommunication = true
	)
	public static abstract class ClusterTestWithProxy extends ClusterTestBase {
	}

	@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
	@ExtendWith(MyTestWatcher.class)
	public static abstract class ClusterTestBase {

		protected abstract Collection<URI> getClusterUris();

		final Logger log = LoggerFactory.getLogger(this.getClass());

		protected void doWithRoutingDriver(Collection<URI> uris, Consumer<Driver> fn) {
			doWithRoutingDriver(uris, d -> {
				fn.accept(d);
				return (Void) null;
			});
		}

		protected <T> T doWithRoutingDriver(Collection<URI> uris, Function<Driver, T> fn) {
			try (Driver driver = GraphDatabase
				.routingDriver(uris, AuthTokens.basic("neo4j", "password"), Config.defaultConfig())) {
				assertDoesNotThrow(driver::verifyConnectivity);
				return fn.apply(driver);
			}
		}

		protected void doWithBoltOnEveryUri(Collection<URI> uris, Consumer<Driver> fn) {
			// TODO: parallelise this
			doWithBoltOnEveryUri(uris, d -> {
				fn.accept(d);
				return (Void) null;
			});
		}

		protected <T> List<T> doWithBoltOnEveryUri(Collection<URI> uris, Function<Driver, T> fn) {
			// TODO: parallelise this
			Config driverConfig = Config.builder()
				.withConnectionTimeout(5, TimeUnit.SECONDS)
				.withMaxConnectionPoolSize(1)
				.withMaxTransactionRetryTime(0, TimeUnit.SECONDS)
				.build();
			return uris.stream().map(uri -> {
				try (Driver driver = GraphDatabase
					.driver(uri.toString().replace("neo4j://", "bolt://"),
						AuthTokens.basic("neo4j", "password"), driverConfig)) {
					return fn.apply(driver);
				}
			}).collect(Collectors.toList());
		}

		void verifyServersStoppedGracefully(Collection<Neo4jServer> containers) {
			log.info(() -> "verifying servers did stop gracefully");
			containers.forEach(container -> {
				String latestLogs = container.getLatestLogs();
				assertThat(latestLogs).contains(GRACEFUL_SHUTDOWN_SUCCESS_LOG);
				assertThat(latestLogs).endsWith(GRACEFUL_SHUTDOWN_SUCCESS_LOG);
			});
		}

		void verifyServersDidNotStopGracefully(Collection<Neo4jServer> containers) {
			log.info(() -> "verifying servers did not stop gracefully");
			containers.forEach(container -> {
				String latestLogs = container.getLatestLogs();
				assertThat(latestLogs).doesNotContain(GRACEFUL_SHUTDOWN_SUCCESS_LOG);
				assertThat(latestLogs).doesNotEndWith(GRACEFUL_SHUTDOWN_SUCCESS_LOG);
			});
		}

		protected Collection<URI> getClusterUrisExcluding(Set<Neo4jServer> exclusions) {
			Set<URI> excluding = exclusions.stream().map(Neo4jServer::getURI).collect(Collectors.toSet());
			return getClusterUris().stream().filter(u -> !excluding.contains(u)).collect(Collectors.toList());
		}

		protected void verifyRoutingTable(Collection<URI> uris, int expectedSize, boolean expectedWritable)
			throws InterruptedException, TimeoutException {
			verifyRoutingTable(uris, expectedSize, expectedWritable, CORE_REJOIN_TIMEOUT);
		}

		protected void verifyRoutingTable(Collection<URI> uris, int expectedSize, boolean expectedWritable,
			Duration timeout)
			throws InterruptedException, TimeoutException {

			// Shuffle these so that we don't always try the same core for the routing driver on every test run
			// n.b. we will use the same core within the loop below and that is deliberate
			uris = (uris instanceof List<?>) ? uris : new ArrayList<>(uris);
			Collections.shuffle((List<?>) uris);

			String urisLogString = uris.stream().map(URI::toString)
				.collect(Collectors.joining(","));
			log.info(() -> "verifying routing table for: " + urisLogString);

			Instant start = Instant.now();
			Instant deadline = start.plus(timeout);
			String lastOutput = "";
			while (Instant.now().isBefore(deadline)) {
				try (Driver driver = GraphDatabase
					.routingDriver(uris, AuthTokens.basic("neo4j", "password"), Config.defaultConfig())) {
					assertDoesNotThrow(driver::verifyConnectivity);

					// TODO: Check that every available core agrees about the routing table
					try (Session session = driver.session()) {
						final String routingOutput = checkRoutingTable(session, expectedSize, expectedWritable);
						log.info(() -> "Routing table check passed " + routingOutput);
						lastOutput = routingOutput;
					}
					try (Session session = driver.session(SessionConfig.forDatabase("system"))) {
						final String showDatabasesOutput = checkShowDatabases(session, expectedSize * 2,
							expectedWritable);
						log.info(() -> "SHOW DATABASES check passed " + showDatabasesOutput);
						lastOutput = showDatabasesOutput;
					}
					log.info(
						() -> "Routing table verified after: " + Duration.between(start, Instant.now()).toString());
					return;
				} catch (SessionExpiredException | ServiceUnavailableException | AssertionError e) {
					final String message = "RoutingTable problem:" + e.getMessage() + "\nLast output: " + lastOutput;
					log.warn(() -> message);
					Thread.sleep(TRANSACTION_RETRY_INTERVAL_MILLISECONDS);
				}
			}
			throw new TimeoutException("Unable to verify routing table within " + timeout.toString());
		}

		private String checkRoutingTable(Session session, int expectedSize, boolean expectedWritable) {

			final TransactionWork<List<Record>> getRoutingTable = tx -> {
				Result r = tx.run("CALL dbms.routing.getRoutingTable({}, \"neo4j\")");
				List<Record> records = r.list();
				assertThat(records).hasSize(1);
				return records;
			};

			List<Record> records = session.readTransaction(getRoutingTable);
			RoutingTable routingTable = RoutingTable.FromQueryResult(records);

			assertThat(routingTable.distinctAddresses)
				.as("Routing table too small", routingTable)
				.hasSize(expectedSize);

			if (expectedWritable) {

				assertThat(routingTable.writers).hasSize(1);

				// Query the routing table using a write transaction - this should go to a different server than the read transaction
				RoutingTable routingTableFromWriter = RoutingTable
					.FromQueryResult(session.writeTransaction(getRoutingTable));
				assertThat(routingTableFromWriter).isEqualTo(routingTable);
			}

			return routingTable.toString();
		}

		protected void logAkkaState() {
			List<String> akkaStates = doWithBoltOnEveryUri(getClusterUris(), d -> {
				try (Session session = d.session()) {
					Map<String, Value> result = session.readTransaction(getAkkaState);
					return "{\n" + result.entrySet().stream()
						.map(k -> "  " + k.getKey() + ":" + k.getValue().toString()).collect(
							Collectors.joining("\n")) + "\n}";
				} catch (SessionExpiredException | ServiceUnavailableException e) {
					log.warn(() -> "Unable to get akka state because of: " + e.getMessage());
				}
				return null;
			});
			final String akkaStateString = akkaStates.stream().filter(Objects::nonNull)
				.collect(Collectors.joining("\n"));
			log.info(
				() -> "Akka state (results from " + akkaStates.stream().filter(Objects::nonNull).count() + "cores): "
					+ akkaStateString);
		}

		protected void checkAkkaState(Collection<URI> uris, int expectedSize) {

			List<AkkaState> akkaStates = doWithBoltOnEveryUri(uris, d -> {
				try (Session session = d.session()) {
					Map<String, Value> result = session.readTransaction(getAkkaState);
					return AkkaState.FromQueryResult(result);
				}
			});

			final String akkaStateString = akkaStates.stream().distinct().map(AkkaState::toString)
				.collect(Collectors.joining());

			log.info(() -> "Akka state: " + akkaStateString);

			assertThat(akkaStates.stream().map(s -> s.up).distinct()).hasSize(1);
			assertThat(akkaStates.stream().map(s -> s.up).distinct().findFirst().get()).hasSize(expectedSize);
			assertThat(akkaStates.stream().map(s -> s.members).distinct()).hasSize(1);
			assertThat(akkaStates.stream().map(s -> s.members).distinct().findFirst().get()).hasSize(expectedSize);
			assertThat(akkaStates.stream().map(s -> s.notUp).distinct()).hasSize(1);
			assertThat(akkaStates.stream().map(s -> s.notUp).distinct().findFirst().get()).hasSize(0);
			assertThat(akkaStates.stream().map(s -> s.unreachable).distinct()).hasSize(1);
			assertThat(akkaStates.stream().map(s -> s.unreachable).distinct().findFirst().get()).hasSize(0);

			log.info(() -> "Akka state checks passed.");
		}

		protected void assertClusterUsable(Collection<URI> uris, int expectedSize)
			throws InterruptedException, TimeoutException {
			log.info(() -> "checking cluster usability");
			Instant start = Instant.now();
			Instant deadline = start.plus(DB_ONLINE_TIMEOUT);
			while (Instant.now().isBefore(deadline)) {
				try (Driver driver = GraphDatabase
					.routingDriver(uris, AuthTokens.basic("neo4j", "password"), Config.defaultConfig())) {
					assertDoesNotThrow(driver::verifyConnectivity);

					try (Session session = driver.session()) {
						checkDummyWriteTx(session);
						log.info(() -> "Cluster accepts write transactions");

						checkDummyReadTx(session);
						log.info(() -> "Cluster accepts read transactions");
					}
					try (Session session = driver.session(SessionConfig.forDatabase("system"))) {
						checkShowDatabases(session, expectedSize * 2, true);
						log.info(() -> "DBMS reports that it is healthy");
					}
					return;
				} catch (SessionExpiredException | ServiceUnavailableException | AssertionError e) {
					log.warn(e, () -> "Connectivity problem:");
					Thread.sleep(10000);
				}
			}

			throw new TimeoutException("Unable to verify connectivity");
		}
	}

	private static class RoutingTable {
		private final List<String> distinctAddresses;
		private final List<String> writers;

		public RoutingTable(List<String> distinctAddresses, List<String> writers) {

			// Sort these so that equality will work
			distinctAddresses.sort(String::compareTo);
			writers.sort(String::compareTo);

			this.distinctAddresses = distinctAddresses;
			this.writers = writers;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			RoutingTable that = (RoutingTable) o;
			return distinctAddresses.equals(that.distinctAddresses) &&
				writers.equals(that.writers);
		}

		@Override
		public int hashCode() {
			return Objects.hash(distinctAddresses, writers);
		}

		@Override public String toString() {
			return "RoutingTable{" +
				"distinctAddresses=" + distinctAddresses +
				", writers=" + writers +
				'}';
		}

		static RoutingTable FromQueryResult(List<Record> records) {
			assert records.size() == 1;
			List<Map<String, Value>> servers = records.get(0).get("servers").asList(Values.ofMap(Values.ofValue()));
			List<String> distinctAddresses = servers.stream()
				.flatMap(m -> m.get("addresses").asList(Values.ofString()).stream()).distinct().collect(
					Collectors.toList());
			List<String> writers = servers.stream().filter(m -> m.get("role").asString().equalsIgnoreCase("write"))
				.flatMap(m -> m.get("addresses").asList(Values.ofString()).stream()).distinct()
				.collect(Collectors.toList());

			return new RoutingTable(distinctAddresses, writers);
		}

	}

	private static class AkkaState {
		private final String selfAddress;
		private final String leader;
		private final Set<String> members;
		private final Set<String> unreachable;
		private final Set<String> up;
		private final Set<String> notUp;

		private AkkaState(String selfAddress, String leader, Set<String> members, Set<String> unreachable,
			Set<String> up, Set<String> notUp) {
			this.selfAddress = selfAddress;
			this.leader = leader;
			this.members = members;
			this.unreachable = unreachable;
			this.up = up;
			this.notUp = notUp;
		}

		@Override public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			AkkaState akkaState = (AkkaState) o;
			return // selfAddress.equals(akkaState.selfAddress) && don't include self address in equality
				leader.equals(akkaState.leader) &&
					members.equals(akkaState.members) &&
					unreachable.equals(akkaState.unreachable) &&
					up.equals(akkaState.up) &&
					notUp.equals(akkaState.notUp);
		}

		@Override
		public int hashCode() {
			return Objects.hash(
				//selfAddress, don't include self address in hash
				leader, members, unreachable, up, notUp
			);
		}

		static AkkaState FromQueryResult(Map<String, Value> result) {
			assert result.size() == 7;
			//Gson g = new Gson();
			JsonParser p = new JsonParser();
			JsonObject clusterStatus = p.parse(result.get("ClusterStatus").asString()).getAsJsonObject();
			JsonArray clusterStateMembers = clusterStatus.get("members").getAsJsonArray();

			Set<String> members = Arrays.stream(result.get("Members").asString().split(",")).filter(m -> !m.isEmpty())
				.collect(Collectors.toSet());

			assertThat(members).isEqualTo(StreamSupport.stream(clusterStateMembers.spliterator(), false)
				.map(m -> m.getAsJsonObject().get("address").getAsString())
				.filter(m -> !m.isEmpty()).collect(Collectors.toSet()));

			Set<String> unreachable = Arrays.stream(result.get("Unreachable").asString().split(","))
				.filter(m -> !m.isEmpty()).collect(Collectors.toSet());

			assertThat(unreachable)
				.isEqualTo(StreamSupport.stream(clusterStatus.get("unreachable").getAsJsonArray().spliterator(), false)
					.map(m -> m.getAsJsonObject().get("node").getAsString())
					.filter(m -> !m.isEmpty()).collect(Collectors.toSet()));

			Set<String> up = StreamSupport.stream(clusterStateMembers.spliterator(), false)
				.filter(m -> m.getAsJsonObject().get("status").getAsString().equalsIgnoreCase("Up"))
				.map(m -> m.getAsJsonObject().get("address").getAsString())
				.filter(m -> !m.isEmpty())
				.collect(Collectors.toSet());

			Set<String> notUp = StreamSupport.stream(clusterStateMembers.spliterator(), false)
				.filter(m -> !m.getAsJsonObject().get("status").getAsString().equalsIgnoreCase("Up"))
				.map(m -> m.getAsJsonObject().get("address").getAsString())
				.filter(m -> !m.isEmpty())
				.collect(Collectors.toSet());

			return new AkkaState(
				clusterStatus.get("self-address").getAsString(),
				result.get("Leader").asString(),
				members,
				unreachable,
				up,
				notUp
			);
		}

		@Override public String toString() {
			return "AkkaState{" +
				"selfAddress='" + selfAddress + '\'' +
				", leader='" + leader + '\'' +
				", members=" + members +
				", unreachable=" + unreachable +
				", up=" + up +
				", notUp=" + notUp +
				'}';
		}
	}

	public static class MyTestWatcher implements TestWatcher {
		@Override
		public void testAborted(ExtensionContext extensionContext, Throwable throwable) {
			// do something
		}

		@Override
		public void testDisabled(ExtensionContext extensionContext, Optional<String> optional) {
			// do something
		}

		@Override
		public void testFailed(ExtensionContext extensionContext, Throwable throwable) {
			// do something
			ClusterTestBase test = (ClusterTestBase) extensionContext.getRequiredTestInstance();
			test.logAkkaState();
		}

		@Override
		public void testSuccessful(ExtensionContext extensionContext) {
			// do something
			ClusterTestBase test = (ClusterTestBase) extensionContext.getRequiredTestInstance();
			test.logAkkaState();
		}
	}
}
