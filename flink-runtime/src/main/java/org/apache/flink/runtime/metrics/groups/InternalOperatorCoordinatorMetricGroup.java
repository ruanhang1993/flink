package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;

import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class InternalOperatorCoordinatorMetricGroup
        extends ComponentMetricGroup<JobManagerJobMetricGroup>
        implements OperatorCoordinatorMetricGroup {
    private final String operatorName;
    private final OperatorID operatorID;

    private final Counter numEventsIn;
    private final Counter numEventsOut;

    public InternalOperatorCoordinatorMetricGroup(
            MetricRegistry registry,
            JobManagerJobMetricGroup parent,
            OperatorID operatorID,
            String operatorName) {
        super(
                registry,
                registry.getScopeFormats()
                        .getOperatorCoordinatorScopeFormat()
                        .formatScope(checkNotNull(parent), operatorID, operatorName),
                parent);
        this.operatorID = operatorID;
        this.operatorName = operatorName;
        numEventsIn = counter(MetricNames.NUM_EVENTS_IN);
        numEventsOut = counter(MetricNames.NUM_EVENTS_OUT);
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "coordinator";
    }

    @Override
    protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
        return new QueryScopeInfo.JobQueryScopeInfo(this.parent.jobId.toString());
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return Collections.emptyList();
    }

    @Override
    public Counter getNumEventsInCounter() {
        return numEventsIn;
    }

    @Override
    public Counter getNumEventsOutCounter() {
        return numEventsOut;
    }
}
