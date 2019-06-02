package com.openinc.targetgroupsdk.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

/**
 * Created by Sai Ravi Teja K on Friday 22, Mar 2019
 **/

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetTargetGroupAttributesRequest {
    private Collection<String> entityIds;
}
