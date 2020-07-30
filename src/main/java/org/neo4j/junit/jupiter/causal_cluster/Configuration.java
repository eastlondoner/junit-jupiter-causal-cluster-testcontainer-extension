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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.With;

import java.io.Serializable;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * The clusters configuration.
 *
 * @author Michael J. Simons
 */
@RequiredArgsConstructor
@Getter
@With
@ToString
class Configuration implements Serializable {

	private final String neo4jVersion;

	private final int numberOfCoreMembers;

	private final int numberOfReadReplicas;

	private final Duration startupTimeout;

	private final String password;

	private final int initialHeapSize;

	private final int pagecacheSize;

	private final boolean toxiproxyEnabled;

	/**
	 * Optional custom image name. Has precedence of the version number if set.
	 */
	private final String customImageName;

	/**
	 * Get the core index and hostname for all configured cores
	 * @return A stream mapping a core index (integer) -> hostname (String)
	 */
	Stream<Map.Entry<Integer, String>> iterateCoreMembers() {
		final IntFunction<String> generateInstanceName = i -> String.format("neo4j%d", i);

		return IntStream.rangeClosed(1, numberOfCoreMembers)
			.mapToObj(i -> new AbstractMap.SimpleEntry<>(i - 1, generateInstanceName.apply(i)));
	}

	String getImageName() {
		return Optional.ofNullable(customImageName).filter(s -> !s.isEmpty())
			.orElseGet(() -> String.format("neo4j:%s-enterprise", neo4jVersion));
	}

	/**
	 * Optional neo4j bin folder. Overrides the contents of the neo4j docker image.
	 */
	private final String neo4jSourceOverride;
}
