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

package org.apache.flink.connectors.test.common.external;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.job.JobDetailsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;

/** The client is used to get job metrics by rest API. */
public class MetricQueryRestClient {
    private static final Logger LOG = LoggerFactory.getLogger(MetricQueryRestClient.class);
    private RestClient restClient;

    public MetricQueryRestClient(Configuration configuration, Executor executor)
            throws ConfigurationException {
        restClient = new RestClient(configuration, executor);
    }

    public JobDetailsInfo getJobDetails(TestEnvironment.Endpoint endpoint, JobID jobId)
            throws Exception {
        String jmAddress = endpoint.getAddress();
        int jmPort = endpoint.getPort();

        final JobDetailsHeaders detailsHeaders = JobDetailsHeaders.getInstance();
        final JobMessageParameters params = new JobMessageParameters();
        params.jobPathParameter.resolve(jobId);

        return restClient
                .sendRequest(
                        jmAddress, jmPort, detailsHeaders, params, EmptyRequestBody.getInstance())
                .get(30, TimeUnit.SECONDS);
    }

    public Double getMetricByRestApi(
            TestEnvironment.Endpoint endpoint,
            JobID jobId,
            String sourceOrSinkName,
            String metricName)
            throws Exception {
        String jmAddress = endpoint.getAddress();
        int jmPort = endpoint.getPort();

        // get job details, including the vertex id
        JobMessageParameters jobMessageParameters = new JobMessageParameters();
        jobMessageParameters.jobPathParameter.resolve(jobId);
        JobDetailsInfo jobDetailsInfo =
                restClient
                        .sendRequest(
                                jmAddress,
                                jmPort,
                                JobDetailsHeaders.getInstance(),
                                jobMessageParameters,
                                EmptyRequestBody.getInstance())
                        .get(30, TimeUnit.SECONDS);

        // get the vertex id for source
        JobDetailsInfo.JobVertexDetailsInfo vertex =
                jobDetailsInfo.getJobVertexInfos().stream()
                        .filter(v -> v.getName().contains(sourceOrSinkName))
                        .findAny()
                        .orElse(null);
        assertFalse(vertex == null);
        JobVertexID vertexId = vertex.getJobVertexID();

        // get metric name
        AggregatedSubtaskMetricsParameters subtaskMetricsParameters =
                new AggregatedSubtaskMetricsParameters();
        Iterator<MessagePathParameter<?>> pathParams =
                subtaskMetricsParameters.getPathParameters().iterator();
        for (int i = 0; i < 2; i++) {
            MessagePathParameter<?> pathParam = pathParams.next();
            if (i == 0) {
                ((JobIDPathParameter) pathParam).resolve(jobId);
            } else {
                ((JobVertexIdPathParameter) pathParam).resolve(vertexId);
            }
        }
        AggregatedMetricsResponseBody metricsResponseBody =
                restClient
                        .sendRequest(
                                jmAddress,
                                jmPort,
                                AggregatedSubtaskMetricsHeaders.getInstance(),
                                subtaskMetricsParameters,
                                EmptyRequestBody.getInstance())
                        .get(30, TimeUnit.SECONDS);

        // query metric value
        String queryParam =
                metricsResponseBody.getMetrics().stream()
                        .filter(
                                m ->
                                        m.getId().endsWith(metricName)
                                                && m.getId().contains(sourceOrSinkName))
                        .map(m -> m.getId())
                        .collect(Collectors.joining(","));

        if (StringUtils.isNullOrWhitespaceOnly(queryParam)) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot find metric[%s] for operator [%s].",
                            metricName, sourceOrSinkName));
        }

        MetricsFilterParameter metricFilter =
                (MetricsFilterParameter)
                        subtaskMetricsParameters.getQueryParameters().iterator().next();
        metricFilter.resolveFromString(queryParam);

        metricsResponseBody =
                restClient
                        .sendRequest(
                                jmAddress,
                                jmPort,
                                AggregatedSubtaskMetricsHeaders.getInstance(),
                                subtaskMetricsParameters,
                                EmptyRequestBody.getInstance())
                        .get(30, TimeUnit.SECONDS);

        return metricsResponseBody.getMetrics().iterator().next().getSum();
    }
}
