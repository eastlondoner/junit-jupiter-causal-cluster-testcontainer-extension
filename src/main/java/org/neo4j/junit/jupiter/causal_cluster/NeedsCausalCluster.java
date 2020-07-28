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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Registers the Causal Cluster extension.
 *
 * @author Michael J. Simons
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@ExtendWith(CausalClusterExtension.class)
public @interface NeedsCausalCluster {
	int numberOfCoreMembers() default CausalClusterExtension.DEFAULT_NUMBER_OF_CORE_MEMBERS;

	/**
	 * @return A valid Neo4j version number.
	 */
	String neo4jVersion() default CausalClusterExtension.DEFAULT_NEO4J_VERSION;

	/**
	 * @return A completely custom image name. Must refer to a Neo4j enterprise image.
	 */
	String customImageName() default "";

	/**
	 * @return Startup timeout for the cluster. Defaults to 5 minutes.
	 */
	long startupTimeOutInMillis() default CausalClusterExtension.DEFAULT_STARTUP_TIMEOUT_IN_MILLIS;

	String password() default "password";

	boolean proxyInternalCommunication() default false;

	/**
	 * @return A path to a local folder containing neo4j binaries to use. Must refer to Neo4j enterprise binaries.
	 */
	String overrideWithLocalNeo4jSource() default "";
}
