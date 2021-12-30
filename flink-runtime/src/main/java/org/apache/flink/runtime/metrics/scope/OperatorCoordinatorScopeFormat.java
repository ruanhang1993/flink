package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;

public class OperatorCoordinatorScopeFormat extends ScopeFormat {
    public OperatorCoordinatorScopeFormat(String format, JobManagerJobScopeFormat parentFormat) {
        super(
                format,
                parentFormat,
                new String[] {
                    SCOPE_HOST, SCOPE_JOB_ID, SCOPE_JOB_NAME, SCOPE_OPERATOR_ID, SCOPE_OPERATOR_NAME
                });
    }

    public String[] formatScope(
            JobManagerJobMetricGroup parent, OperatorID operatorID, String operatorName) {

        final String[] template = copyTemplate();
        final String[] values = {
            parent.parent().hostname(),
            valueOrNull(parent.jobId()),
            valueOrNull(parent.jobName()),
            valueOrNull(operatorID),
            valueOrNull(operatorName)
        };
        return bindVariables(template, values);
    }
}
