package com.openinc.targetgroupsdk.businessObjects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Sets;
import lombok.*;

import java.util.Set;

/**
 * Created by Sai Ravi Teja K on Monday 25, Mar 2019
 **/

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TargetGroupBO {

    private String groupId;
    private String type;
    private String status;
    private String purpose;

    private String name;
    private String description;
    private String owner;

    private Set<String> includeIds = Sets.newHashSet();
    private Set<String> excludeIds = Sets.newHashSet();

    private String includeConditionExpression;
    private String excludeConditionExpression;
}
