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

package org.apache.flink.connectors.test.common.testsuites;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connectors.test.common.environment.ExecutionEnvironmentOptions;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.external.MetricQueryRestClient;
import org.apache.flink.connectors.test.common.external.sink.DataStreamSinkExternalContext;
import org.apache.flink.connectors.test.common.external.sink.TestingSinkOptions;
import org.apache.flink.connectors.test.common.junit.extensions.ConnectorTestingExtension;
import org.apache.flink.connectors.test.common.junit.extensions.TestCaseInvocationContextProvider;
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;
import org.apache.flink.connectors.test.common.source.ListSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.connectors.test.common.utils.TestDataMatchers.matchesMultipleSplitTestData;
import static org.apache.flink.connectors.test.common.utils.TestUtils.doubleEquals;
import static org.apache.flink.connectors.test.common.utils.TestUtils.getResultData;
import static org.apache.flink.connectors.test.common.utils.TestUtils.waitAndExecute;
import static org.apache.flink.runtime.testutils.CommonTestUtils.terminateJob;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForJobStatus;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Base class for sink test suite.
 *
 * <p>All cases should have well-descriptive JavaDoc, including:
 *
 * <ul>
 *   <li>What's the purpose of this case
 *   <li>Simple description of how this case works
 *   <li>Condition to fulfill in order to pass this case
 *   <li>Requirement of running this case
 * </ul>
 */
@ExtendWith({
    ConnectorTestingExtension.class,
    TestLoggerExtension.class,
    TestCaseInvocationContextProvider.class
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Experimental
public abstract class SinkTestSuiteBase<T extends Comparable<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(SinkTestSuiteBase.class);
    static ExecutorService executorService =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    private final long jobExecuteTimeMs = 5000;

    // ----------------------------- Basic test cases ---------------------------------

    /**
     * Test connector sink with only one split in the external system.
     *
     * <p>This test will create one split in the external system, write test data into it, and
     * consume back via a Flink job with 1 parallelism.
     */
    @TestTemplate
    @DisplayName("Test sink")
    public void testSink(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        TestingSinkOptions sinkOptions = getTestingSinkOptions(semantic);
        final List<T> testRecords = generateTestData(sinkOptions, externalContext);

        // Build and execute Flink job
        StreamExecutionEnvironment execEnv =
                testEnv.getExecutionEnvironment(
                        new ExecutionEnvironmentOptions(externalContext.getConnectorJarPaths()));
        execEnv.enableCheckpointing(50);
        execEnv.fromSource(
                        new ListSource<>(
                                Boundedness.CONTINUOUS_UNBOUNDED, testRecords, testRecords.size()),
                        WatermarkStrategy.noWatermarks(),
                        "sourceTest")
                .setParallelism(1)
                .returns(externalContext.getTestDataTypeInformation())
                .sinkTo(getSink(externalContext, sinkOptions))
                .name("sinkTest");
        final JobClient jobClient = execEnv.executeAsync("Sink Test");

        try {
            waitForJobStatus(
                    jobClient,
                    Collections.singletonList(JobStatus.RUNNING),
                    Deadline.fromNow(Duration.ofSeconds(30)));

            // wait committer to commit and wait external system to finish writing
            Thread.sleep(jobExecuteTimeMs);

            // Check test result
            List<T> target = sort(testRecords);
            List<T> result =
                    sort(
                            getResultData(
                                    externalContext.createSinkDataReader(sinkOptions),
                                    testRecords,
                                    30,
                                    semantic));
            judgeResultBySemantic(result.iterator(), Arrays.asList(target), semantic);
        } finally {
            killJob(jobClient);
        }
    }

    /**
     * Test connector source restart from a completed checkpoint.
     *
     * <p>This test will write test data to external system and consume back via a Flink job with
     * parallelism 2. Then stop the job, restart the job from the completed checkpoint. After the
     * job has been running, add some extra data to the source and compare the result.
     *
     * <p>The number of records consumed by Flink need to be identical to the test data written into
     * the external system to pass this test. There's no requirement for record order.
     */
    @TestTemplate
    @DisplayName("Test sink restarting from a savepoint")
    public void testSavepoint(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        restartFromSavepoint(testEnv, externalContext, semantic, 2, 2);
    }

    /**
     * Test connector source restart from a completed checkpoint with a higher parallelism.
     *
     * <p>This test will write test data to external system and consume back via a Flink job with
     * parallelism 2. Then stop the job, restart the job from the completed checkpoint with a lower
     * parallelism 4. After the job has been running, add some extra data to the source and compare
     * the result.
     *
     * <p>The number of records consumed by Flink need to be identical to the test data written into
     * the external system to pass this test. There's no requirement for record order.
     */
    @TestTemplate
    @DisplayName("Test sink restarting with a higher parallelism")
    public void testScaleUp(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        restartFromSavepoint(testEnv, externalContext, semantic, 2, 4);
    }

    /**
     * Test connector source restart from a completed checkpoint with a lower parallelism.
     *
     * <p>This test will write test data to external system and consume back via a Flink job with
     * parallelism 4. Then stop the job, restart the job from the completed checkpoint with a lower
     * parallelism 2. After the job has been running, add some extra data to the source and compare
     * the result.
     *
     * <p>The number of records consumed by Flink need to be identical to the test data written into
     * the external system to pass this test. There's no requirement for record order.
     */
    @TestTemplate
    @DisplayName("Test sink restarting with a lower parallelism")
    public void testScaleDown(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        restartFromSavepoint(testEnv, externalContext, semantic, 4, 2);
    }

    private void restartFromSavepoint(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            DeliveryGuarantee semantic,
            final int beforeParallelism,
            final int afterParallelism)
            throws Exception {
        TestingSinkOptions sinkOptions = getTestingSinkOptions(semantic);
        final StreamExecutionEnvironment execEnv =
                testEnv.getExecutionEnvironment(
                        new ExecutionEnvironmentOptions(externalContext.getConnectorJarPaths()));
        execEnv.setRestartStrategy(RestartStrategies.noRestart());

        final List<T> testRecords = generateTestData(sinkOptions, externalContext);
        int numBeforeSuccess = testRecords.size() / 2;
        DataStreamSource<T> source =
                execEnv.fromSource(
                                new ListSource<>(
                                        Boundedness.CONTINUOUS_UNBOUNDED,
                                        testRecords,
                                        numBeforeSuccess),
                                WatermarkStrategy.noWatermarks(),
                                "beforeRestartSource")
                        .setParallelism(1);

        source.returns(externalContext.getTestDataTypeInformation())
                .sinkTo(getSink(externalContext, sinkOptions))
                .name("Sink restart test")
                .setParallelism(beforeParallelism);
        final JobClient jobClient = execEnv.executeAsync("Restart Test");

        // ------------------------------- Wait job to fail --------------------------- ----

        waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.RUNNING),
                Deadline.fromNow(Duration.ofSeconds(30)));

        // wait committer to commit and wait external system to finish writing
        Thread.sleep(jobExecuteTimeMs);

        String savepointDir =
                jobClient
                        .stopWithSavepoint(true, testEnv.getCheckpointUri())
                        .get(30, TimeUnit.SECONDS);
        waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.FINISHED),
                Deadline.fromNow(Duration.ofSeconds(30)));

        List<T> target = sort(testRecords.subList(0, numBeforeSuccess));
        List<T> result =
                sort(
                        getResultData(
                                externalContext.createSinkDataReader(sinkOptions),
                                target,
                                30,
                                semantic));
        judgeResultBySemantic(result.iterator(), Arrays.asList(target), semantic, false);

        // -------------------------------- Restart job -------------------------------------------
        final StreamExecutionEnvironment restartEnv =
                testEnv.getExecutionEnvironment(
                        new ExecutionEnvironmentOptions(
                                externalContext.getConnectorJarPaths(), savepointDir));
        restartEnv.enableCheckpointing(50);

        DataStreamSource<T> restartSource =
                restartEnv
                        .fromSource(
                                new ListSource<>(
                                        Boundedness.CONTINUOUS_UNBOUNDED,
                                        testRecords,
                                        testRecords.size()),
                                WatermarkStrategy.noWatermarks(),
                                "restartSource")
                        .setParallelism(1);

        restartSource
                .returns(externalContext.getTestDataTypeInformation())
                .sinkTo(getSink(externalContext, sinkOptions))
                .setParallelism(afterParallelism);
        final JobClient restartJobClient = restartEnv.executeAsync("Restart Test");

        // wait committer to commit and wait external system to finish writing
        Thread.sleep(jobExecuteTimeMs);
        killJob(restartJobClient);

        // Check the result
        target = sort(testRecords);
        result =
                sort(
                        getResultData(
                                externalContext.createSinkDataReader(sinkOptions),
                                testRecords,
                                30,
                                semantic));
        judgeResultBySemantic(result.iterator(), Arrays.asList(target), semantic);
    }

    /**
     * Test connector sink metrics.
     *
     * <p>This test will write test data to the external system, and consume back via a Flink job
     * with parallelism 2. Then read and compare the metrics.
     *
     * <p>Now test: numRecordsOut
     */
    @TestTemplate
    @DisplayName("Test sink metrics")
    public void testMetrics(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        TestingSinkOptions sinkOptions = getTestingSinkOptions(semantic);
        int parallelism = 2;
        final List<T> testRecords = generateTestData(sinkOptions, externalContext);

        // make sure use different names when executes multi times
        String sinkName = "metricTestSink" + testRecords.hashCode();
        final StreamExecutionEnvironment env =
                testEnv.getExecutionEnvironment(
                        new ExecutionEnvironmentOptions(externalContext.getConnectorJarPaths()));
        env.enableCheckpointing(50);

        DataStreamSource<T> source =
                env.fromSource(
                                new ListSource<>(
                                        Boundedness.CONTINUOUS_UNBOUNDED,
                                        testRecords,
                                        testRecords.size()),
                                WatermarkStrategy.noWatermarks(),
                                "metricTestSource")
                        .setParallelism(1);

        source.returns(externalContext.getTestDataTypeInformation())
                .sinkTo(getSink(externalContext, sinkOptions))
                .name(sinkName)
                .setParallelism(parallelism);
        final JobClient jobClient = env.executeAsync("Metrics Test");
        final MetricQueryRestClient queryRestClient =
                new MetricQueryRestClient(new Configuration(), executorService);
        try {
            waitForAllTaskRunning(
                    () ->
                            queryRestClient.getJobDetails(
                                    testEnv.getRestEndpoint(), jobClient.getJobID()),
                    Deadline.fromNow(Duration.ofSeconds(30)));

            waitAndExecute(
                    () -> {
                        // test metrics
                        try {
                            assertSinkMetrics(
                                    queryRestClient,
                                    testEnv,
                                    jobClient.getJobID(),
                                    sinkName,
                                    0,
                                    testRecords.size(),
                                    parallelism,
                                    false);
                        } catch (Exception e) {
                            throw new IllegalArgumentException("Error in assert metrics.", e);
                        }
                    },
                    5000);
        } finally {
            // Clean up
            killJob(jobClient);
        }
    }

    // ----------------------------- Helper Functions ---------------------------------

    /**
     * Generate a set of test records.
     *
     * @param testingSinkOptions
     * @param externalContext External context
     * @return Collection of generated test records
     */
    protected List<T> generateTestData(
            TestingSinkOptions testingSinkOptions,
            DataStreamSinkExternalContext<T> externalContext) {
        return externalContext.generateTestData(
                testingSinkOptions, ThreadLocalRandom.current().nextLong());
    }

    /**
     * Compare the test data with the result.
     *
     * @param resultIterator the data read from the job
     * @param testData the test data
     * @param semantic the supported semantic, see {@link DeliveryGuarantee}
     */
    private void judgeResultBySemantic(
            Iterator<T> resultIterator, List<List<T>> testData, DeliveryGuarantee semantic) {
        assertThat(resultIterator).satisfies(matchesMultipleSplitTestData(testData, semantic));
    }

    private void judgeResultBySemantic(
            Iterator<T> resultIterator,
            List<List<T>> testData,
            DeliveryGuarantee semantic,
            boolean testDataAllInResult) {
        assertThat(resultIterator)
                .satisfies(matchesMultipleSplitTestData(testData, semantic, testDataAllInResult));
    }

    /** Compare the metrics. */
    private void assertSinkMetrics(
            MetricQueryRestClient queryRestClient,
            TestEnvironment testEnv,
            JobID jobId,
            String sinkName,
            long allBytes,
            long allRecordSize,
            int parallelism,
            boolean hasTimestamps)
            throws Exception {
        double sumNumRecordsOut =
                queryRestClient.getMetricByRestApi(
                        testEnv.getRestEndpoint(), jobId, sinkName, MetricNames.IO_NUM_RECORDS_OUT);
        assertThat(doubleEquals(allRecordSize, sumNumRecordsOut)).isTrue();
    }

    /** Sort the list. */
    private List<T> sort(List<T> list) {
        return list.stream().sorted().collect(Collectors.toList());
    }

    /** Sort the nested list. */
    private List<List<T>> sortNestList(List<List<T>> list) {
        return list.stream()
                .map(l -> l.stream().sorted().collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    private TestingSinkOptions getTestingSinkOptions(DeliveryGuarantee deliveryGuarantee) {
        return TestingSinkOptions.builder().withDeliveryGuarantee(deliveryGuarantee).build();
    }

    private void killJob(JobClient jobClient) throws Exception {
        terminateJob(jobClient, Duration.ofSeconds(30));
        waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.CANCELED),
                Deadline.fromNow(Duration.ofSeconds(30)));
    }

    private Sink<T, ?, ?, ?> getSink(
            DataStreamSinkExternalContext<T> context, TestingSinkOptions sinkOptions) {
        try {
            return context.createSink(sinkOptions);
        } catch (UnsupportedOperationException e) {
            // abort the test
            throw new TestAbortedException("Not support this test.", e);
        }
    }
}
