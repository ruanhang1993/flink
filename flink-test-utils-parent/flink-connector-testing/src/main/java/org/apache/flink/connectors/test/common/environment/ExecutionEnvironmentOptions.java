/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.test.common.environment;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;

import java.net.URL;
import java.util.List;

/**
 * Options for configuring {@link StreamExecutionEnvironment} created by {@link TestEnvironment}.
 */
public class ExecutionEnvironmentOptions {
    private final List<URL> connectorJarPaths;
    @Nullable private final String savepointRestorePath;

    public ExecutionEnvironmentOptions(
            List<URL> connectorJarPaths, @Nullable String savepointRestorePath) {
        this.connectorJarPaths = connectorJarPaths;
        this.savepointRestorePath = savepointRestorePath;
    }

    /** List of connector JARs paths. */
    public List<URL> connectorJarPaths() {
        return connectorJarPaths;
    }

    /** Path of savepoint that the job should recover from. */
    @Nullable
    public String savepointRestorePath() {
        return savepointRestorePath;
    }
}
