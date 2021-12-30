package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistry;

import static org.apache.flink.runtime.metrics.MetricNames.UNASSIGNED_SPLITS;

public class InternalSplitEnumeratorMetricGroup extends InternalOperatorCoordinatorMetricGroup
        implements SplitEnumeratorMetricGroup {
    public InternalSplitEnumeratorMetricGroup(
            MetricRegistry registry,
            JobManagerJobMetricGroup parent,
            OperatorID operatorID,
            String operatorName) {
        super(registry, parent, operatorID, operatorName);
    }

    @Override
    public <G extends Gauge<Long>> G setUnassignedSplitsGauge(G unassignedSplitsGauge) {
        return gauge(UNASSIGNED_SPLITS, unassignedSplitsGauge);
    }
}
