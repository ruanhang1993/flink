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
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connectors.test.common.source.split.ListSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.core.io.InputStatus.MORE_AVAILABLE;

/** The reader reads data from a list. */
public class ListSourceReader<T> implements SourceReader<T, ListSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(ListSourceReader.class);

    private final int successNum;
    private volatile int numElementsEmitted;
    private volatile boolean successCk = false;
    private volatile boolean isRunning = true;

    /** The context of this source reader. */
    protected SourceReaderContext context;

    private List<T> elements;
    private Boundedness boundedness;
    private Counter numRecordInCounter;

    public ListSourceReader(
            int successNum,
            List<T> elements,
            Boundedness boundedness,
            SourceReaderContext context) {
        this.successNum = successNum;
        this.context = context;
        this.numElementsEmitted = 0;
        this.elements = elements;
        this.boundedness = boundedness;
        this.numRecordInCounter = context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();
    }

    @Override
    public void start() {}

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (isRunning && numElementsEmitted < elements.size()) {
            if (numElementsEmitted < successNum || successCk) {
                output.collect(elements.get(numElementsEmitted));
                numElementsEmitted++;
                numRecordInCounter.inc();
            }
            return MORE_AVAILABLE;
        }

        if (Boundedness.CONTINUOUS_UNBOUNDED.equals(boundedness)) {
            return MORE_AVAILABLE;
        } else {
            // wait for checkpoints to end
            return InputStatus.END_OF_INPUT;
        }
    }

    @Override
    public List<ListSplit> snapshotState(long checkpointId) {
        if (!successCk && numElementsEmitted == successNum) {
            successCk = true;
            LOG.info("checkpoint {} is the target checkpoint to be used.", checkpointId);
        }
        return Arrays.asList(new ListSplit(numElementsEmitted));
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return FutureCompletingBlockingQueue.AVAILABLE;
    }

    @Override
    public void addSplits(List<ListSplit> splits) {
        numElementsEmitted = splits.get(0).getEmitNum();
        LOG.info("ListSourceReader restores from {}.", numElementsEmitted);
    }

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void close() throws Exception {
        isRunning = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.info("{} checkpoint finished.", checkpointId);
    }
}
