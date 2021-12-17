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

package org.apache.flink.connectors.test.common.external.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connectors.test.common.external.ExternalContext;

import java.util.List;

/**
 * External context for DataStream sinks.
 *
 * @param <T> Type of elements before serialization by sink
 */
public interface DataStreamSinkExternalContext<T> extends ExternalContext {

    /**
     * Create an instance of {@link Sink} satisfying given options.
     *
     * @param sinkOptions options of the sink
     * @throws UnsupportedOperationException if the provided option is not supported.
     */
    Sink<T, ?, ?, ?> createSink(TestingSinkOptions sinkOptions)
            throws UnsupportedOperationException;

    /** Create a reader for consuming data written to the external system by sink. */
    SinkDataReader<T> createSinkDataReader(TestingSinkOptions sinkOptions);

    /**
     * Generate test data.
     *
     * <p>These test data will be sent to sink via a special source in Flink job, write to external
     * system by sink, consume back via {@link SinkDataReader}, and make comparison with {@link
     * T#equals(Object)} for validating correctness.
     *
     * <p>Make sure that the {@link T#equals(Object)} returns false when the records in different
     * splits.
     *
     * @param sinkOptions options of the sink
     * @param seed Seed for generating random test data set.
     * @return List of generated test data.
     */
    List<T> generateTestData(TestingSinkOptions sinkOptions, long seed);

    /** Get type information of the generated test data. */
    TypeInformation<T> getTestDataTypeInformation();
}
