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

package org.apache.flink.connectors.test.common.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connectors.test.common.source.enumerator.MockEnumState;
import org.apache.flink.connectors.test.common.source.enumerator.MockEnumStateSerializer;
import org.apache.flink.connectors.test.common.source.enumerator.MockEnumerator;
import org.apache.flink.connectors.test.common.source.split.ListSplit;
import org.apache.flink.connectors.test.common.source.split.ListSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.List;

/**
 * The source reads data from a list and stops reading at the fixed position. The source will wait
 * until the checkpoint or savepoint triggers.
 *
 * <p>Note that this source must be of parallelism 1.
 */
public class ListSource<OUT> implements Source<OUT, ListSplit, MockEnumState> {
    // Boundedness
    private final Boundedness boundedness;

    private List<OUT> elements;

    private final int successNum;

    public ListSource(Boundedness boundedness, List<OUT> elements, int successNum) {
        if (successNum > elements.size()) {
            throw new RuntimeException("SuccessNum must be larger than elements' size.");
        }
        this.boundedness = boundedness;
        this.elements = elements;
        this.successNum = successNum;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<OUT, ListSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new ListSourceReader<>(successNum, elements, boundedness, readerContext);
    }

    @Override
    public SplitEnumerator<ListSplit, MockEnumState> createEnumerator(
            SplitEnumeratorContext<ListSplit> enumContext) throws Exception {
        return new MockEnumerator();
    }

    @Override
    public SplitEnumerator<ListSplit, MockEnumState> restoreEnumerator(
            SplitEnumeratorContext<ListSplit> enumContext, MockEnumState checkpoint)
            throws Exception {
        return new MockEnumerator();
    }

    @Override
    public SimpleVersionedSerializer<ListSplit> getSplitSerializer() {
        return new ListSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<MockEnumState> getEnumeratorCheckpointSerializer() {
        return new MockEnumStateSerializer();
    }
}
