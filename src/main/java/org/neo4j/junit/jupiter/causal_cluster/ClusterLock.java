package org.neo4j.junit.jupiter.causal_cluster;

/*
 * Copyright 2015-2020 the original author or authors.
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v2.0 which
 * accompanies this distribution and is available at
 *
 * https://www.eclipse.org/legal/epl-v20.html
 */

import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.lang.annotation.Annotation;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;

/**
 * @since 1.3
 */
public class ClusterLock
{
    private static final int TEST_PARALLELISM = Integer.parseInt(System.getProperty("org.neo4j.junit.jupiter.causal_cluster.test_parallelism", "2"));
    private static final Semaphore lock = new Semaphore(TEST_PARALLELISM);

    public static void acquire() throws InterruptedException
    {
        lock.acquire();
    }

    public static void release()
    {
        lock.release();
    }
}
