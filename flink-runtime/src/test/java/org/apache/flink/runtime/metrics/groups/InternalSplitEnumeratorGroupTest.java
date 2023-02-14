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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link InternalSplitEnumeratorMetricGroup}. */
@ExtendWith(TestLoggerExtension.class)
public class InternalSplitEnumeratorGroupTest {

    private MetricRegistryImpl registry;

    @BeforeEach
    public void setup() {
        registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());
    }

    @AfterEach
    public void teardown() throws Exception {
        if (registry != null) {
            registry.closeAsync().get();
        }
    }

    @Test
    void testGenerateScopeDefault() {
        final JobID jobId = new JobID();
        final JobVertexID jobVertexId = new JobVertexID();
        final OperatorID operatorId = new OperatorID();
        JobManagerOperatorMetricGroup jmJobGroup =
                JobManagerMetricGroup.createJobManagerMetricGroup(registry, "localhost")
                        .addJob(jobId, "myJobName")
                        .getOrAddOperator(jobVertexId, "taskName", operatorId, "opName");
        InternalOperatorCoordinatorMetricGroup operatorCoordinatorMetricGroup =
                new InternalOperatorCoordinatorMetricGroup(jmJobGroup);
        InternalSplitEnumeratorMetricGroup splitEnumeratorMetricGroup =
                new InternalSplitEnumeratorMetricGroup(
                        operatorCoordinatorMetricGroup, "split-enum");

        assertThat(splitEnumeratorMetricGroup.getScopeComponents())
                .containsExactly(
                        "localhost",
                        "jobmanager",
                        "myJobName",
                        "opName",
                        "coordinator",
                        "split-enum");
        assertThat(splitEnumeratorMetricGroup.getMetricIdentifier("name"))
                .isEqualTo("localhost.jobmanager.myJobName.opName.coordinator.split-enum.name");
    }
}
