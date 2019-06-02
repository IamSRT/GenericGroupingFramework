package com.openinc.targetgroupsdk.businessObjects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Builder;

import java.util.Map;

/**
 * Created by Sai Ravi Teja K on Thursday 21, Mar 2019
 **/

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TargetGroupAttributesEntity {

    private String targetGroupEntityId;

    private String type;

    private Map<String, Object> attributeValueMap;

    public void addAttribute(String attribute, Object value){
        if(this.attributeValueMap == null) attributeValueMap = Maps.newHashMap();
        attributeValueMap.put(attribute, value);
    }
}
