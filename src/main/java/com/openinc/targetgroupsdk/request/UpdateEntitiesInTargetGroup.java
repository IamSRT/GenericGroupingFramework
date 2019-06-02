package com.openinc.targetgroupsdk.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

/**
 * Created by Sai Ravi Teja K on Tuesday 26, Mar 2019
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class UpdateEntitiesInTargetGroup {
    Set<String> entityIdsToAdd;
    Set<String> entityIdsToRemove;
    String groupId;
}
