package org.apache.flink.connector.base.source.reader.mocks;

import org.apache.flink.connector.base.source.reader.RecordEvaluator;

import java.util.function.Function;

/** A mock RecordEvaluator class. */
public class MockRecordEvaluator implements RecordEvaluator<Integer> {
    private final Function<Integer, Boolean> func;

    public MockRecordEvaluator(Function<Integer, Boolean> func) {
        this.func = func;
    }

    @Override
    public boolean isEndOfStream(Integer record) {
        return func.apply(record);
    }
}
