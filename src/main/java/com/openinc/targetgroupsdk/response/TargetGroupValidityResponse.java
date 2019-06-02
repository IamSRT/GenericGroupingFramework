package com.openinc.targetgroupsdk.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

/**
 * Created by Sai Ravi Teja K on Tuesday 26, Mar 2019
 **/

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TargetGroupValidityResponse {
    private boolean isValid;
    private String errorMessage;
}
