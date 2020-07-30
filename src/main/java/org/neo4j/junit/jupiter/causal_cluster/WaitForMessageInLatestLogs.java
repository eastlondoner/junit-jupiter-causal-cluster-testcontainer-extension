package org.neo4j.junit.jupiter.causal_cluster;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.utility.LogUtils;

public class WaitForMessageInLatestLogs extends AbstractWaitStrategy {

	// TODO: make this a regex?
	private final String query;
	private final Neo4jServer server;

	public WaitForMessageInLatestLogs(String query, Neo4jServer server) {
		this.query = query;
		this.server = server;
	}

	@Override
	protected void waitUntilReady() {

		String latestLogs = server.getLatestLogs();
		String lastLine = latestLogs.substring(latestLogs.lastIndexOf("\n", latestLogs.length() - 2)).trim();

		WaitingConsumer waitingConsumer = new WaitingConsumer();
		LogUtils
			.followOutput(DockerClientFactory.instance().client(), this.waitStrategyTarget.getContainerId(),
				waitingConsumer);

		Predicate<OutputFrame> ignoreBefore = (outputFrame) -> {
			return outputFrame.getUtf8String().contains(lastLine);
		};

		Predicate<OutputFrame> waitPredicate = (outputFrame) -> {
			return outputFrame.getUtf8String().contains(query);
		};

		try {
			waitingConsumer.waitUntil(ignoreBefore, 1, TimeUnit.SECONDS, 1);
			waitingConsumer.waitUntil(waitPredicate, this.startupTimeout.getSeconds(), TimeUnit.SECONDS, 1);
		} catch (TimeoutException var4) {
			throw new ContainerLaunchException(
				"Timed out waiting for log output matching '" + query + "'");
		}
	}
}
