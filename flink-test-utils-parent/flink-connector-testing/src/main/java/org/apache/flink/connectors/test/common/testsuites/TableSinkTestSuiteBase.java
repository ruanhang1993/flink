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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.environment.TestEnvironmentSettings;
import org.apache.flink.connectors.test.common.external.sink.TableSinkExternalContext;
import org.apache.flink.connectors.test.common.external.sink.TestingSinkSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.connectors.test.common.utils.TestUtils.containSameVal;
import static org.apache.flink.connectors.test.common.utils.TestUtils.appendResultData;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Base class for table sink test suites. */
public abstract class TableSinkTestSuiteBase extends AbstractTableTestSuiteBase {
    private static final Logger LOG = LoggerFactory.getLogger(TableSinkTestSuiteBase.class);

    private static final int NUM_RECORDS_UPPER_BOUND = 200;
    private static final int NUM_RECORDS_LOWER_BOUND = 100;

    /**
     * Test data types for connector table sink.
     *
     * <p>This test will insert records to the sink table, and read back.
     */
    @TestTemplate
    @DisplayName("Test table sink basic write")
    public void testBaseWrite(
            TestEnvironment testEnv,
            TableSinkExternalContext externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        testTableTypes(testEnv, externalContext, semantic, Arrays.asList(DataTypes.STRING()));
    }

    /**
     * Test data types for connector table sink.
     *
     * <p>This test will insert records to the sink table, and read back.
     *
     * <p>Now only test basic types.
     */
    @TestTemplate
    @DisplayName("Test table sink data type")
    public void testTableDataType(
            TestEnvironment testEnv,
            TableSinkExternalContext externalContext,
            DeliveryGuarantee semantic)
            throws Exception {
        testTableTypes(testEnv, externalContext, semantic, supportTypes());
    }

    private void testTableTypes(
            TestEnvironment testEnv,
            TableSinkExternalContext externalContext,
            DeliveryGuarantee semantic,
            List<DataType> supportTypes)
            throws Exception {
        TestingSinkSettings sinkOptions = getTestingSinkOptions(semantic);
        StreamExecutionEnvironment env =
                testEnv.createExecutionEnvironment(
                        TestEnvironmentSettings.builder()
                                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                                .build());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Map<String, String> tableOptions = getTableOptions(externalContext, sinkOptions);
        String tableName = "TableSinkTest" + semantic.toString().replaceAll("-", "");
        tEnv.executeSql(getCreateTableSql(tableName, tableOptions, supportTypes));

        Tuple2<String, List<RowData>> tuple2 =
                initialValues(
                        tableName,
                        ThreadLocalRandom.current().nextLong(),
                        NUM_RECORDS_UPPER_BOUND,
                        NUM_RECORDS_LOWER_BOUND,
                        supportTypes);
        String initialValuesSql = tuple2.f0;
        tEnv.executeSql(initialValuesSql).await();

        List<RowData> expectedResult = tuple2.f1;
        assertThat(
                        containSameVal(
                                expectedResult,
                                appendResultData(
                                        new ArrayList<>(),
                                        externalContext.createSinkRowDataReader(
                                                sinkOptions, getTableSchema(supportTypes)),
                                        expectedResult,
                                        30,
                                        semantic),
                                semantic))
                .isTrue();
    }

    private TestingSinkSettings getTestingSinkOptions(DeliveryGuarantee deliveryGuarantee) {
        return TestingSinkSettings.builder().setDeliveryGuarantee(deliveryGuarantee).build();
    }

    private Map<String, String> getTableOptions(
            TableSinkExternalContext externalContext, TestingSinkSettings sinkSettings) {
        try {
            return externalContext.getSinkTableOptions(sinkSettings);
        } catch (UnsupportedOperationException e) {
            // abort the test
            throw new TestAbortedException("Not support this test.", e);
        }
    }
}
