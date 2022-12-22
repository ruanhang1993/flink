package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.connector.base.source.reader.RecordEvaluator;
import org.apache.flink.table.data.RowData;

/** A mock record evaluator. */
public class MockRecordEvaluator implements RecordEvaluator<RowData> {
    @Override
    public boolean isEndOfStream(RowData record) {
        return record == null || record.getString(0).toString().contains("End");
    }
}
