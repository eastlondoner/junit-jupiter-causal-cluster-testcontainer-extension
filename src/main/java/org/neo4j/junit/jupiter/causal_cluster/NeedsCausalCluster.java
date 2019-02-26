/*
 * Copyright (c) 2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.junit.jupiter.causal_cluster;

import java.lang.annotation.ElementType;
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
@ExtendWith(CausalClusterExtension.class)
public @interface NeedsCausalCluster {
	int numberOfCoreMembers() default CausalClusterExtension.DEFAULT_NUMBER_OF_CORE_MEMBERS;

	String neo4jVersion() default CausalClusterExtension.DEFAULT_NEO4J_VERSION;

	/**
	 * Startup timeout for the cluster. Defaults to 5 minutes.
	 */
	long startupTimeOutInMillis() default CausalClusterExtension.DEFAULT_STARTUP_TIMEOUT_IN_MILLIS;

	String password() default "password";
}
