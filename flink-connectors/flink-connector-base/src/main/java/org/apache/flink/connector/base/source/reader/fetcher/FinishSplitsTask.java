package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsDeletion;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** The task to finish reading some splits. */
public class FinishSplitsTask<SplitT extends SourceSplit> implements SplitFetcherTask {
    private final SplitReader<?, SplitT> splitReader;
    private final List<SplitT> finishedSplits;
    private final Map<String, SplitT> assignedSplits;
    private final Consumer<Collection<String>> splitFinishedCallback;

    FinishSplitsTask(
            SplitReader<?, SplitT> splitReader,
            List<SplitT> finishedSplits,
            Map<String, SplitT> assignedSplits,
            Consumer<Collection<String>> splitFinishedCallback) {
        this.splitReader = splitReader;
        this.finishedSplits = finishedSplits;
        this.assignedSplits = assignedSplits;
        this.splitFinishedCallback = splitFinishedCallback;
    }

    @Override
    public boolean run() {
        for (SplitT s : finishedSplits) {
            assignedSplits.remove(s.splitId());
        }
        splitReader.handleSplitsChanges(new SplitsDeletion<>(finishedSplits));
        splitFinishedCallback.accept(
                finishedSplits.stream().map(SourceSplit::splitId).collect(Collectors.toList()));
        return true;
    }

    @Override
    public void wakeUp() {
        // Do nothing.
    }

    @Override
    public String toString() {
        return String.format("FinishSplitsTask: [%s]", finishedSplits);
    }
}
