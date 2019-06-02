package com.openinc.targetgroupsdk.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.openinc.targetgroupsdk.businessObjects.TargetGroupBO;
import lombok.Data;

/**
 * Created by Sai Ravi Teja K on Tuesday 26, Mar 2019
 **/

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TargetGroupAsyncRequest {
    private String jobId;
    private String rowId;
    private String rowExecutionId;
    private TargetGroupBO targetGroupBO;
}
