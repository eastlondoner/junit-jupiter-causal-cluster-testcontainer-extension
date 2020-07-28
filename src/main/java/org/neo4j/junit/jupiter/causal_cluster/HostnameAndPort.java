package org.neo4j.junit.jupiter.causal_cluster;

import lombok.NonNull;

import java.util.Objects;

final class HostnameAndPort {
	private final String hostname;
	private final int port;

	HostnameAndPort(@NonNull String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}

	@Override public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		HostnameAndPort that = (HostnameAndPort) o;
		return port == that.port &&
			hostname.equals(that.hostname);
	}

	@Override
	public int hashCode() {
		return Objects.hash(hostname, port);
	}

	@Override
	public String toString() {
		return String.format("%s:%s", hostname, port);
	}

}
