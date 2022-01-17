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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Test environment for running test on a remote Flink cluster. */
@Experimental
public class RemoteClusterTestEnvironment implements TestEnvironment {

    private final String host;
    private final int port;
    private final String checkpointUri;
    private final List<String> jarPaths = new ArrayList<>();
    private final Configuration config;

    /**
     * Construct a test environment for a remote Flink cluster.
     *
     * @param host Hostname of the remote JobManager
     * @param port REST port of the remote JobManager
     * @param jarPaths Path of JARs to be shipped to Flink cluster
     */
    public RemoteClusterTestEnvironment(
            String host, int port, String checkpointUri, String... jarPaths) {
        this(host, port, checkpointUri, new Configuration(), jarPaths);
    }

    /**
     * Construct a test environment for a remote Flink cluster with configurations.
     *
     * @param host Hostname of the remote JobManager
     * @param port REST port of the remote JobManager
     * @param config Configurations of the test environment
     * @param jarPaths Path of JARs to be shipped to Flink cluster
     */
    public RemoteClusterTestEnvironment(
            String host, int port, String checkpointUri, Configuration config, String... jarPaths) {
        this.host = host;
        this.port = port;
        this.checkpointUri = checkpointUri;
        this.config = config;
        this.jarPaths.addAll(Arrays.asList(jarPaths));
    }

    @Override
    public StreamExecutionEnvironment createExecutionEnvironment(
            TestEnvironmentSettings envOptions) {
        jarPaths.addAll(
                envOptions.getConnectorJarPaths().stream()
                        .map(URL::getPath)
                        .collect(Collectors.toList()));
        return StreamExecutionEnvironment.createRemoteEnvironment(
                host, port, config, jarPaths.toArray(new String[0]));
    }

    @Override
    public void startUp() {}

    @Override
    public void tearDown() {}

    @Override
    public Endpoint getRestEndpoint() {
        return new Endpoint(host, port);
    }

    @Override
    public String getCheckpointUri() {
        return checkpointUri;
    }
}
