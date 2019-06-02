package com.openinc.targetgroupsdk.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by sajalagarwal on 26/02/19.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class IngestionJobRowUpdateEvent {
    private String jobId;
    private String rowId;
    private String rowExecutionId;
    private String logToAppend;
    private String status;
}
