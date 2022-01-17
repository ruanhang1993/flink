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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.environment.TestEnvironmentSettings;
import org.apache.flink.connectors.test.common.external.sink.DataStreamSinkExternalContext;
import org.apache.flink.connectors.test.common.external.sink.ExternalSystemDataReader;
import org.apache.flink.connectors.test.common.external.sink.TestingSinkSettings;
import org.apache.flink.connectors.test.common.junit.extensions.ConnectorTestingExtension;
import org.apache.flink.connectors.test.common.junit.extensions.TestCaseInvocationContextProvider;
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;
import org.apache.flink.connectors.test.common.source.ListSource;
import org.apache.flink.connectors.test.common.utils.MetricQueryer;
import org.apache.flink.connectors.test.common.utils.TestDataMatchers;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.connectors.test.common.utils.TestDataMatchers.matchesMultipleSplitTestData;
import static org.apache.flink.connectors.test.common.utils.TestUtils.appendResultData;
import static org.apache.flink.connectors.test.common.utils.TestUtils.doubleEquals;
import static org.apache.flink.connectors.test.common.utils.TestUtils.timeoutAssert;
import static org.apache.flink.runtime.testutils.CommonTestUtils.terminateJob;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForJobStatus;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;

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

    private final long jobExecuteTimeMs = 20000;

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
        TestingSinkSettings sinkSettings = getTestingSinkSettings(semantic);
        final List<T> testRecords = generateTestData(sinkSettings, externalContext);

        // Build and execute Flink job
        StreamExecutionEnvironment execEnv =
                testEnv.createExecutionEnvironment(
                        TestEnvironmentSettings.builder()
                                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                                .build());
        execEnv.enableCheckpointing(50);
        execEnv.fromCollection(testRecords)
                .name("sourceTest")
                .setParallelism(1)
                .returns(externalContext.getProducedType())
                .sinkTo(tryCreateSink(externalContext, sinkSettings))
                .name("sinkTest");
        final JobClient jobClient = execEnv.executeAsync("Sink Test");

        waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.FINISHED),
                Deadline.fromNow(Duration.ofSeconds(30)));

        // Check test result
        List<T> target = sort(testRecords);
        checkResult(externalContext.createSinkDataReader(sinkSettings), target, semantic);
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
        TestingSinkSettings sinkSettings = getTestingSinkSettings(semantic);
        final StreamExecutionEnvironment execEnv =
                testEnv.createExecutionEnvironment(
                        TestEnvironmentSettings.builder()
                                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                                .build());
        execEnv.setRestartStrategy(RestartStrategies.noRestart());

        final List<T> testRecords = generateTestData(sinkSettings, externalContext);
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

        source.returns(externalContext.getProducedType())
                .sinkTo(tryCreateSink(externalContext, sinkSettings))
                .name("Sink restart test")
                .setParallelism(beforeParallelism);
        CollectResultIterator<T> iterator = addCollectSink(source);
        final JobClient jobClient = execEnv.executeAsync("Restart Test");
        iterator.setJobClient(jobClient);

        // ------------------------------- Wait job to fail --------------------------- ----
        String savepointDir;
        try {
            final MetricQueryer queryRestClient =
                    new MetricQueryer(new Configuration(), executorService);
            waitForAllTaskRunning(
                    () ->
                            queryRestClient.getJobDetails(
                                    testEnv.getRestEndpoint(), jobClient.getJobID()),
                    Deadline.fromNow(Duration.ofSeconds(30)));

            timeoutAssert(
                    executorService,
                    () -> {
                        int count = 0;
                        while (count < numBeforeSuccess && iterator.hasNext()) {
                            iterator.next();
                            count++;
                        }
                        if (count < numBeforeSuccess) {
                            throw new IllegalStateException(
                                    String.format("Fail to get %d records.", numBeforeSuccess));
                        }
                    },
                    30,
                    TimeUnit.SECONDS);
            savepointDir =
                    jobClient
                            .stopWithSavepoint(true, testEnv.getCheckpointUri())
                            .get(30, TimeUnit.SECONDS);
            waitForJobStatus(
                    jobClient,
                    Collections.singletonList(JobStatus.FINISHED),
                    Deadline.fromNow(Duration.ofSeconds(30)));
        } catch (Exception e) {
            killJob(jobClient);
            throw e;
        }

        List<T> target = sort(testRecords.subList(0, numBeforeSuccess));
        checkResult(externalContext.createSinkDataReader(sinkSettings), target, semantic, false);

        // -------------------------------- Restart job -------------------------------------------
        final StreamExecutionEnvironment restartEnv =
                testEnv.createExecutionEnvironment(
                        TestEnvironmentSettings.builder()
                                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                                .setSavepointRestorePath(savepointDir)
                                .build());
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
                .returns(externalContext.getProducedType())
                .sinkTo(tryCreateSink(externalContext, sinkSettings))
                .setParallelism(afterParallelism);
        addCollectSink(restartSource);
        final JobClient restartJobClient = restartEnv.executeAsync("Restart Test");

        try {
            // Check the result
            checkResult(
                    externalContext.createSinkDataReader(sinkSettings),
                    sort(testRecords),
                    semantic);
        } finally {
            killJob(restartJobClient);
            iterator.close();
        }
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
        TestingSinkSettings sinkSettings = getTestingSinkSettings(semantic);
        int parallelism = 2;
        final List<T> testRecords = generateTestData(sinkSettings, externalContext);

        // make sure use different names when executes multi times
        String sinkName = "metricTestSink" + testRecords.hashCode();
        final StreamExecutionEnvironment env =
                testEnv.createExecutionEnvironment(
                        TestEnvironmentSettings.builder()
                                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                                .build());
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

        source.returns(externalContext.getProducedType())
                .sinkTo(tryCreateSink(externalContext, sinkSettings))
                .name(sinkName)
                .setParallelism(parallelism);
        final JobClient jobClient = env.executeAsync("Metrics Test");
        final MetricQueryer queryRestClient =
                new MetricQueryer(new Configuration(), executorService);
        try {
            waitForAllTaskRunning(
                    () ->
                            queryRestClient.getJobDetails(
                                    testEnv.getRestEndpoint(), jobClient.getJobID()),
                    Deadline.fromNow(Duration.ofSeconds(30)));

            waitUntilCondition(
                    () -> {
                        // test metrics
                        try {
                            return compareSinkMetrics(
                                    queryRestClient,
                                    testEnv,
                                    jobClient.getJobID(),
                                    sinkName,
                                    0,
                                    testRecords.size(),
                                    parallelism,
                                    false);
                        } catch (Exception e) {
                            // skip failed assert try
                            return false;
                        }
                    },
                    Deadline.fromNow(Duration.ofMillis(jobExecuteTimeMs)));
        } finally {
            // Clean up
            killJob(jobClient);
        }
    }

    // ----------------------------- Helper Functions ---------------------------------

    /**
     * Generate a set of test records.
     *
     * @param testingSinkSettings sink settings
     * @param externalContext External context
     * @return Collection of generated test records
     */
    protected List<T> generateTestData(
            TestingSinkSettings testingSinkSettings,
            DataStreamSinkExternalContext<T> externalContext) {
        return externalContext.generateTestData(
                testingSinkSettings, ThreadLocalRandom.current().nextLong());
    }

    /**
     * Compare the test data with the result.
     *
     * @param reader the data reader for the sink
     * @param testData the test data
     * @param semantic the supported semantic, see {@link DeliveryGuarantee}
     */
    private void checkResult(
            ExternalSystemDataReader<T> reader, List<T> testData, DeliveryGuarantee semantic)
            throws Exception {
        checkResult(reader, testData, semantic, true);
    }

    private void checkResult(
            ExternalSystemDataReader<T> reader,
            List<T> testData,
            DeliveryGuarantee semantic,
            boolean testDataAllInResult)
            throws Exception {
        final ArrayList<T> result = new ArrayList<>();
        final TestDataMatchers.MultipleSplitDataMatcher<T> matcher =
                matchesMultipleSplitTestData(
                        Arrays.asList(testData), semantic, testDataAllInResult);
        waitUntilCondition(
                () -> {
                    appendResultData(result, reader, testData, 30, semantic);
                    return matcher.matches(sort(result).iterator());
                },
                Deadline.fromNow(Duration.ofMillis(jobExecuteTimeMs)));
    }

    /** Compare the metrics. */
    private boolean compareSinkMetrics(
            MetricQueryer queryRestClient,
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
        return doubleEquals(allRecordSize, sumNumRecordsOut);
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

    private TestingSinkSettings getTestingSinkSettings(DeliveryGuarantee deliveryGuarantee) {
        return TestingSinkSettings.builder().setDeliveryGuarantee(deliveryGuarantee).build();
    }

    private void killJob(JobClient jobClient) throws Exception {
        terminateJob(jobClient, Duration.ofSeconds(30));
        waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.CANCELED),
                Deadline.fromNow(Duration.ofSeconds(30)));
    }

    private Sink<T, ?, ?, ?> tryCreateSink(
            DataStreamSinkExternalContext<T> context, TestingSinkSettings sinkSettings) {
        try {
            return context.createSink(sinkSettings);
        } catch (UnsupportedOperationException e) {
            // abort the test
            throw new TestAbortedException("Not support this test.", e);
        }
    }

    protected CollectResultIterator<T> addCollectSink(DataStream<T> stream) {
        TypeSerializer<T> serializer =
                stream.getType().createSerializer(stream.getExecutionConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        CollectStreamSink<T> sink = new CollectStreamSink<>(stream, factory);
        sink.name("Data stream collect sink");
        stream.getExecutionEnvironment().addOperator(sink.getTransformation());
        CollectResultIterator<T> iterator =
                new CollectResultIterator<>(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        stream.getExecutionEnvironment().getCheckpointConfig());
        return iterator;
    }
}
