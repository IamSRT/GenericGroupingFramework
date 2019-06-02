package com.openinc.targetgroupsdk.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Sets;
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
public class TargetGroupUpdatedEvent {
    Set<String> entityIdsToActivate;
    Set<String> entityIdsToDeactivate;
    String groupId;

    public TargetGroupUpdatedEvent(String groupId){
        entityIdsToActivate = Sets.newHashSet();
        entityIdsToDeactivate = Sets.newHashSet();
        this.groupId = groupId;
    }
}
