package com.openinc.targetgroupsdk.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.openinc.targetgroupsdk.businessObjects.TargetGroupAttributesEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by Sai Ravi Teja K on Tuesday 26, Mar 2019
 **/

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetTargetGroupFromAttributesRequest {
    TargetGroupAttributesEntity groupAttributesEntity;
    String groupPurpose;
}
