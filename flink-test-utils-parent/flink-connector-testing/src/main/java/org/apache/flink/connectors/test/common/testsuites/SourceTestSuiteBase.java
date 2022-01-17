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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connectors.test.common.environment.ClusterControllable;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.environment.TestEnvironmentSettings;
import org.apache.flink.connectors.test.common.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connectors.test.common.external.source.ExternalSystemSplitDataWriter;
import org.apache.flink.connectors.test.common.external.source.TestingSourceSettings;
import org.apache.flink.connectors.test.common.junit.extensions.ConnectorTestingExtension;
import org.apache.flink.connectors.test.common.junit.extensions.TestCaseInvocationContextProvider;
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;
import org.apache.flink.connectors.test.common.utils.MetricQueryer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.util.CloseableIterator;

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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connectors.test.common.utils.TestDataMatchers.matchesMultipleSplitTestData;
import static org.apache.flink.connectors.test.common.utils.TestUtils.doubleEquals;
import static org.apache.flink.connectors.test.common.utils.TestUtils.timeoutAssert;
import static org.apache.flink.runtime.testutils.CommonTestUtils.terminateJob;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForJobStatus;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Base class for all test suites.
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
public abstract class SourceTestSuiteBase<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SourceTestSuiteBase.class);
    static ExecutorService executorService =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    // ----------------------------- Basic test cases ---------------------------------

    /**
     * Test connector source with only one split in the external system.
     *
     * <p>This test will create one split in the external system, write test data into it, and
     * consume back via a Flink job with 1 parallelism.
     *
     * <p>The number and order of records consumed by Flink need to be identical to the test data
     * written to the external system in order to pass this test.
     *
     * <p>A bounded source is required for this test.
     */
    @TestTemplate
    @DisplayName("Test source with single split")
    public void testSourceSingleSplit(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        // Step 1: Preparation
        TestingSourceSettings sourceSettings =
                TestingSourceSettings.builder()
                        .setBoundedness(Boundedness.BOUNDED)
                        .setDeliveryGuarantee(semantic)
                        .build();
        TestEnvironmentSettings envSettings =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .build();
        Source<T, ?, ?> source = tryCreateSource(externalContext, sourceSettings);

        // Step 2: Write test data to external system
        List<T> testRecords = generateAndWriteTestData(0, externalContext, sourceSettings);

        // Step 3: Build and execute Flink job
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(envSettings);
        DataStreamSource<T> stream =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Tested Source")
                        .setParallelism(1);
        CollectIteratorBuilder<T> iteratorBuilder = addCollectSink(stream);
        JobClient jobClient = submitJob(execEnv, "Source Single Split Test");

        // Validate test data
        try (CollectResultIterator<T> resultIterator = iteratorBuilder.build(jobClient)) {
            // Check test result
            LOG.info("Checking test results");
            judgeResultBySemantic(resultIterator, Arrays.asList(testRecords), semantic, null);
        }
    }

    /**
     * Test connector source with multiple splits in the external system
     *
     * <p>This test will create 4 splits in the external system, write test data to all splits, and
     * consume back via a Flink job with 4 parallelism.
     *
     * <p>The number and order of records in each split consumed by Flink need to be identical to
     * the test data written into the external system to pass this test. There's no requirement for
     * record order across splits.
     *
     * <p>A bounded source is required for this test.
     */
    @TestTemplate
    @DisplayName("Test source with multiple splits")
    public void testMultipleSplits(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        // Step 1: Preparation
        TestingSourceSettings sourceSettings =
                TestingSourceSettings.builder()
                        .setBoundedness(Boundedness.BOUNDED)
                        .setDeliveryGuarantee(semantic)
                        .build();
        TestEnvironmentSettings envOptions =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .build();
        Source<T, ?, ?> source = tryCreateSource(externalContext, sourceSettings);

        // Step 2: Write test data to external system
        int splitNumber = 4;
        List<List<T>> testRecordsLists = new ArrayList<>();
        for (int i = 0; i < splitNumber; i++) {
            testRecordsLists.add(generateAndWriteTestData(i, externalContext, sourceSettings));
        }

        // Step 3: Build and execute Flink job
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(envOptions);
        DataStreamSource<T> stream =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Tested Source")
                        .setParallelism(splitNumber);
        CollectIteratorBuilder<T> iteratorBuilder = addCollectSink(stream);
        JobClient jobClient = submitJob(execEnv, "Source Multiple Split Test");

        // Step 4: Validate test data
        try (CloseableIterator<T> resultIterator = iteratorBuilder.build(jobClient)) {
            // Check test result
            LOG.info("Checking test results");
            judgeResultBySemantic(resultIterator, testRecordsLists, semantic, null);
        }
    }

    /**
     * Test connector source restart from a savepoint.
     *
     * <p>This test will create 4 splits in the external system first, write test data to all
     * splits, and consume back via a Flink job. Then stop the job with savepoint, restart the job
     * from the checkpoint. After the job has been running, add some extra data to the source and
     * compare the result.
     *
     * <p>The number and order of records in each split consumed by Flink need to be identical to
     * the test data written into the external system to pass this test. There's no requirement for
     * record order across splits.
     */
    @TestTemplate
    @DisplayName("Test source restarting from a savepoint")
    public void testSavepoint(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        restartFromSavepoint(testEnv, externalContext, semantic, 4, 4, 4);
    }

    /**
     * Test connector source restart from a savepoint with a higher parallelism.
     *
     * <p>This test will create 4 splits in the external system first, write test data to all splits
     * and consume back via a Flink job with parallelism 2. Then stop the job with savepoint,
     * restart the job from the checkpoint with a higher parallelism 4. After the job has been
     * running, add some extra data to the source and compare the result.
     *
     * <p>The number and order of records in each split consumed by Flink need to be identical to
     * the test data written into the external system to pass this test. There's no requirement for
     * record order across splits.
     */
    @TestTemplate
    @DisplayName("Test source restarting with a higher parallelism")
    public void testScaleUp(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        restartFromSavepoint(testEnv, externalContext, semantic, 4, 2, 4);
    }

    /**
     * Test connector source restart from a savepoint with a lower parallelism.
     *
     * <p>This test will create 4 splits in the external system first, write test data to all splits
     * and consume back via a Flink job with parallelism 4. Then stop the job with savepoint,
     * restart the job from the checkpoint with a lower parallelism 2. After the job has been
     * running, add some extra data to the source and compare the result.
     *
     * <p>The number and order of records in each split consumed by Flink need to be identical to
     * the test data written into the external system to pass this test. There's no requirement for
     * record order across splits.
     */
    @TestTemplate
    @DisplayName("Test source restarting with a lower parallelism")
    public void testScaleDown(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        restartFromSavepoint(testEnv, externalContext, semantic, 4, 4, 2);
    }

    private void restartFromSavepoint(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            DeliveryGuarantee semantic,
            final int splitNumber,
            final int beforeParallelism,
            final int afterParallelism)
            throws Exception {
        TestingSourceSettings sourceSettings =
                TestingSourceSettings.builder()
                        .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                        .setDeliveryGuarantee(semantic)
                        .build();
        TestEnvironmentSettings envOptions =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .build();

        final StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(envOptions);
        execEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        execEnv.enableCheckpointing(50);
        execEnv.setRestartStrategy(RestartStrategies.noRestart());

        final List<ExternalSystemSplitDataWriter<T>> writers =
                createSourceSplitWriters(splitNumber, externalContext, sourceSettings);
        final List<List<T>> testRecordCollections = new ArrayList<>();
        for (int i = 0; i < splitNumber; i++) {
            testRecordCollections.add(
                    generateTestDataForWriter(externalContext, sourceSettings, i, writers.get(i)));
        }
        DataStreamSource<T> source =
                execEnv.fromSource(
                                tryCreateSource(externalContext, sourceSettings),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(beforeParallelism);

        // ------------------------------------ START---------------------------------------------
        CollectIteratorBuilder<T> iteratorBuilder = addCollectSink(source);
        final JobClient jobClient = execEnv.executeAsync("Restart Test");
        CollectResultIterator<T> iterator = null;
        try {
            iterator = iteratorBuilder.build(jobClient);

            judgeResultBySemantic(
                    iterator,
                    testRecordCollections,
                    semantic,
                    getTestDataSize(testRecordCollections));
        } catch (Exception e) {
            killJob(jobClient);
            throw e;
        }

        // -------------------------------- Stop with savepoint-----------------------------------

        String savepointDir =
                jobClient
                        .stopWithSavepoint(true, testEnv.getCheckpointUri())
                        .get(30, TimeUnit.SECONDS);
        waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.FINISHED),
                Deadline.fromNow(Duration.ofSeconds(30)));

        // -------------------------------- Add new data-------------------------------------------

        final List<List<T>> newTestRecordCollections = new ArrayList<>();
        for (int i = 0; i < splitNumber; i++) {
            newTestRecordCollections.add(
                    generateTestDataForWriter(externalContext, sourceSettings, i, writers.get(i)));
        }

        // -------------------------------- Restart job-------------------------------------------
        TestEnvironmentSettings restartEnvOptions =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .setSavepointRestorePath(savepointDir)
                        .build();
        final StreamExecutionEnvironment restartEnv =
                testEnv.createExecutionEnvironment(restartEnvOptions);
        restartEnv.enableCheckpointing(500);
        restartEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<T> restartSource =
                restartEnv
                        .fromSource(
                                tryCreateSource(externalContext, sourceSettings),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(afterParallelism);

        // ------------------------------------ START---------------------------------------------
        addCollectSink(restartSource);

        final JobClient restartJobClient = restartEnv.executeAsync("Restart Test");
        try {
            iterator.setJobClient(restartJobClient);

            /*
             * Use the same iterator as the previous run, because the CollectStreamSink will snapshot
             * its state and recover from it.
             *
             * The fetcher in CollectResultIterator is responsible for comminicating with
             * the CollectSinkFunction, and deal the result with CheckpointedCollectResultBuffer
             * in EXACTLY_ONCE semantic.
             */
            judgeResultBySemantic(
                    iterator,
                    newTestRecordCollections,
                    semantic,
                    getTestDataSize(newTestRecordCollections));
        } finally {
            // Clean up
            killJob(restartJobClient);
            iterator.close();
        }
    }

    /**
     * Test connector source metrics.
     *
     * <p>This test will create 4 splits in the external system first, write test data to all splits
     * and consume back via a Flink job with parallelism 4. Then read and compare the metrics.
     *
     * <p>Now test: numRecordsIn
     */
    @TestTemplate
    @DisplayName("Test source metrics")
    public void testMetrics(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {

        TestingSourceSettings sourceSettings =
                TestingSourceSettings.builder()
                        .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                        .setDeliveryGuarantee(semantic)
                        .build();
        TestEnvironmentSettings envOptions =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .build();
        final int splitNumber = 4;
        final List<List<T>> testRecordCollections = new ArrayList<>();
        for (int i = 0; i < splitNumber; i++) {
            testRecordCollections.add(generateAndWriteTestData(i, externalContext, sourceSettings));
        }

        // make sure use different names when executes multi times
        String sourceName = "metricTestSource" + testRecordCollections.hashCode();
        final StreamExecutionEnvironment env = testEnv.createExecutionEnvironment(envOptions);
        env.enableCheckpointing(50);
        final DataStreamSource<T> dataStreamSource =
                env.fromSource(
                                tryCreateSource(externalContext, sourceSettings),
                                WatermarkStrategy.noWatermarks(),
                                sourceName)
                        .setParallelism(splitNumber);
        dataStreamSource.addSink(new DiscardingSink<>());
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
                            return assertSourceMetrics(
                                    queryRestClient,
                                    testEnv,
                                    jobClient.getJobID(),
                                    sourceName,
                                    0,
                                    getTestDataSize(testRecordCollections),
                                    splitNumber,
                                    splitNumber,
                                    false);
                        } catch (Exception e) {
                            // skip failed assert try
                            return false;
                        }
                    },
                    Deadline.fromNow(Duration.ofMillis(5000)));
        } finally {
            // Clean up
            killJob(jobClient);
        }
    }

    /**
     * Test connector source with an idle reader.
     *
     * <p>This test will create 4 split in the external system, write test data to all splits, and
     * consume back via a Flink job with 5 parallelism, so at least one parallelism / source reader
     * will be idle (assigned with no splits). If the split enumerator of the source doesn't signal
     * NoMoreSplitsEvent to the idle source reader, the Flink job will never spin to FINISHED state.
     *
     * <p>The number and order of records in each split consumed by Flink need to be identical to
     * the test data written into the external system to pass this test. There's no requirement for
     * record order across splits.
     *
     * <p>A bounded source is required for this test.
     */
    @TestTemplate
    @DisplayName("Test source with at least one idle parallelism")
    public void testIdleReader(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        // Step 1: Preparation
        TestingSourceSettings sourceSettings =
                TestingSourceSettings.builder()
                        .setBoundedness(Boundedness.BOUNDED)
                        .setDeliveryGuarantee(semantic)
                        .build();
        TestEnvironmentSettings envOptions =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .build();
        Source<T, ?, ?> source = tryCreateSource(externalContext, sourceSettings);

        // Step 2: Write test data to external system
        int splitNumber = 4;
        List<List<T>> testRecordsLists = new ArrayList<>();
        for (int i = 0; i < splitNumber; i++) {
            testRecordsLists.add(generateAndWriteTestData(i, externalContext, sourceSettings));
        }

        // Step 3: Build and execute Flink job
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(envOptions);
        DataStreamSource<T> stream =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Tested Source")
                        .setParallelism(splitNumber + 1);
        CollectIteratorBuilder<T> iteratorBuilder = addCollectSink(stream);
        JobClient jobClient = submitJob(execEnv, "Idle Reader Test");

        // Step 4: Validate test data
        try (CloseableIterator<T> resultIterator = iteratorBuilder.build(jobClient)) {
            LOG.info("Checking test results");
            judgeResultBySemantic(resultIterator, testRecordsLists, semantic, null);
        }
    }

    /**
     * Test connector source with task manager failover.
     *
     * <p>This test will create 1 split in the external system, write test record set A into the
     * split, restart task manager to trigger job failover, write test record set B into the split,
     * and terminate the Flink job finally.
     *
     * <p>The number and order of records consumed by Flink should be identical to A before the
     * failover and B after the failover in order to pass the test.
     *
     * <p>An unbounded source is required for this test, since TaskManager failover will be
     * triggered in the middle of the test.
     */
    @TestTemplate
    @DisplayName("Test TaskManager failure")
    public void testTaskManagerFailure(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<T> externalContext,
            ClusterControllable controller,
            DeliveryGuarantee semantic)
            throws Exception {
        // Step 1: Preparation
        TestingSourceSettings sourceSettings =
                TestingSourceSettings.builder()
                        .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                        .setDeliveryGuarantee(semantic)
                        .build();
        TestEnvironmentSettings envOptions =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .build();
        Source<T, ?, ?> source = tryCreateSource(externalContext, sourceSettings);

        // Step 2: Write test data to external system
        int splitIndex = 0;
        List<T> testRecordsBeforeFailure =
                externalContext.generateTestData(
                        sourceSettings, splitIndex, ThreadLocalRandom.current().nextLong());
        ExternalSystemSplitDataWriter<T> externalSystemSplitDataWriter =
                externalContext.createSourceSplitDataWriter(sourceSettings);
        LOG.info(
                "Writing {} records for split {} to external system",
                testRecordsBeforeFailure.size(),
                splitIndex);
        externalSystemSplitDataWriter.writeRecords(testRecordsBeforeFailure);

        // Step 3: Build and execute Flink job
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(envOptions);
        execEnv.enableCheckpointing(50);
        DataStreamSource<T> stream =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Tested Source")
                        .setParallelism(1);
        CollectIteratorBuilder<T> iteratorBuilder = addCollectSink(stream);
        JobClient jobClient = submitJob(execEnv, "TaskManager Failover Test");

        // Step 4: Validate records before killing TaskManagers
        CloseableIterator<T> iterator = iteratorBuilder.build(jobClient);
        LOG.info("Checking records before killing TaskManagers");
        judgeResultBySemantic(
                iterator,
                Arrays.asList(testRecordsBeforeFailure),
                semantic,
                testRecordsBeforeFailure.size());

        // Step 5: Trigger TaskManager failover
        LOG.info("Trigger TaskManager failover");
        controller.triggerTaskManagerFailover(jobClient, () -> {});

        LOG.info("Waiting for job recovering from failure");
        waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.RUNNING),
                Deadline.fromNow(Duration.ofSeconds(30)));

        // Step 6: Write test data again to external system
        List<T> testRecordsAfterFailure =
                externalContext.generateTestData(
                        sourceSettings, splitIndex, ThreadLocalRandom.current().nextLong());
        LOG.info(
                "Writing {} records for split {} to external system",
                testRecordsAfterFailure.size(),
                splitIndex);
        externalSystemSplitDataWriter.writeRecords(testRecordsAfterFailure);

        // Step 7: Validate test result
        LOG.info("Checking records after job failover");
        judgeResultBySemantic(
                iterator,
                Arrays.asList(testRecordsAfterFailure),
                semantic,
                testRecordsAfterFailure.size());

        // Step 8: Clean up
        terminateJob(jobClient, Duration.ofSeconds(30));
        waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.CANCELED),
                Deadline.fromNow(Duration.ofSeconds(30)));
        iterator.close();
    }

    // ----------------------------- Helper Functions ---------------------------------

    /**
     * Generate a set of test records and write it to the given split writer.
     *
     * @param externalContext External context
     * @return List of generated test records
     */
    protected List<T> generateAndWriteTestData(
            int splitIndex,
            DataStreamSourceExternalContext<T> externalContext,
            TestingSourceSettings testingSourceSettings) {
        List<T> testRecords =
                externalContext.generateTestData(
                        testingSourceSettings, splitIndex, ThreadLocalRandom.current().nextLong());
        LOG.info(
                "Writing {} records for split {} to external system",
                testRecords.size(),
                splitIndex);
        externalContext
                .createSourceSplitDataWriter(testingSourceSettings)
                .writeRecords(testRecords);
        return testRecords;
    }

    protected Source<T, ?, ?> tryCreateSource(
            DataStreamSourceExternalContext<T> externalContext,
            TestingSourceSettings sourceOptions) {
        try {
            return externalContext.createSource(sourceOptions);
        } catch (UnsupportedOperationException e) {
            throw new TestAbortedException("Cannot create source satisfying given options", e);
        }
    }

    protected JobClient submitJob(StreamExecutionEnvironment env, String jobName) throws Exception {
        LOG.info("Submitting Flink job to test environment");
        return env.executeAsync(jobName);
    }

    protected CollectIteratorBuilder<T> addCollectSink(DataStream<T> stream) {
        TypeSerializer<T> serializer =
                stream.getType().createSerializer(stream.getExecutionConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        CollectStreamSink<T> sink = new CollectStreamSink<>(stream, factory);
        sink.name("Data stream collect sink");
        stream.getExecutionEnvironment().addOperator(sink.getTransformation());
        return new CollectIteratorBuilder<>(
                operator,
                serializer,
                accumulatorName,
                stream.getExecutionEnvironment().getCheckpointConfig());
    }

    /**
     * Generate a set of split writers.
     *
     * @param externalContext External context
     * @param num the number of writers
     * @return List of created writers
     */
    protected List<ExternalSystemSplitDataWriter<T>> createSourceSplitWriters(
            int num,
            DataStreamSourceExternalContext<T> externalContext,
            TestingSourceSettings sourceSettings) {
        List<ExternalSystemSplitDataWriter<T>> list = new LinkedList<>();
        for (int i = 0; i < num; i++) {
            list.add(externalContext.createSourceSplitDataWriter(sourceSettings));
        }
        return list;
    }

    /**
     * Generate a set of split writers.
     *
     * @param externalContext External context
     * @param splitIndex the split index
     * @param writer the writer to send data
     * @return List of generated test records
     */
    protected List<T> generateTestDataForWriter(
            DataStreamSourceExternalContext<T> externalContext,
            TestingSourceSettings sourceSettings,
            int splitIndex,
            ExternalSystemSplitDataWriter<T> writer) {
        final List<T> testRecordCollection =
                externalContext.generateTestData(
                        sourceSettings, splitIndex, ThreadLocalRandom.current().nextLong());
        LOG.debug("Writing {} records to external system", testRecordCollection.size());
        writer.writeRecords(testRecordCollection);
        return testRecordCollection;
    }

    /**
     * Get the size of test data.
     *
     * @param collections test data
     * @return the size of test data
     */
    protected int getTestDataSize(List<List<T>> collections) {
        int sumSize = 0;
        for (Collection<T> collection : collections) {
            sumSize += collection.size();
        }
        return sumSize;
    }

    /**
     * Compare the test data with the result.
     *
     * <p>If the source is bounded, limit should be null.
     *
     * @param resultIterator the data read from the job
     * @param testData the test data
     * @param semantic the supported semantic, see {@link DeliveryGuarantee}
     * @param limit expected number of the data to read from the job
     */
    private void judgeResultBySemantic(
            CloseableIterator<T> resultIterator,
            List<List<T>> testData,
            DeliveryGuarantee semantic,
            Integer limit) {
        if (limit != null) {
            timeoutAssert(
                    executorService,
                    () ->
                            assertThat(resultIterator)
                                    .satisfies(
                                            matchesMultipleSplitTestData(
                                                    testData, limit, semantic)),
                    30,
                    TimeUnit.SECONDS);
        } else {
            assertThat(resultIterator).satisfies(matchesMultipleSplitTestData(testData, semantic));
        }
    }

    /** Compare the metrics. */
    private boolean assertSourceMetrics(
            MetricQueryer queryRestClient,
            TestEnvironment testEnv,
            JobID jobId,
            String sourceName,
            long allBytes,
            long allRecordSize,
            int parallelism,
            int numSplits,
            boolean hasTimestamps)
            throws Exception {
        Double sumNumRecordsIn =
                queryRestClient.getMetricByRestApi(
                        testEnv.getRestEndpoint(),
                        jobId,
                        sourceName,
                        MetricNames.IO_NUM_RECORDS_IN);
        return doubleEquals(allRecordSize, sumNumRecordsIn);
    }

    private void killJob(JobClient jobClient) throws Exception {
        terminateJob(jobClient, Duration.ofSeconds(30));
        waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.CANCELED),
                Deadline.fromNow(Duration.ofSeconds(30)));
    }

    protected static class CollectIteratorBuilder<T> {

        private final CollectSinkOperator<T> operator;
        private final TypeSerializer<T> serializer;
        private final String accumulatorName;
        private final CheckpointConfig checkpointConfig;

        protected CollectIteratorBuilder(
                CollectSinkOperator<T> operator,
                TypeSerializer<T> serializer,
                String accumulatorName,
                CheckpointConfig checkpointConfig) {
            this.operator = operator;
            this.serializer = serializer;
            this.accumulatorName = accumulatorName;
            this.checkpointConfig = checkpointConfig;
        }

        protected CollectResultIterator<T> build(JobClient jobClient) {
            CollectResultIterator<T> iterator =
                    new CollectResultIterator<>(
                            operator.getOperatorIdFuture(),
                            serializer,
                            accumulatorName,
                            checkpointConfig);
            iterator.setJobClient(jobClient);
            return iterator;
        }
    }
}
