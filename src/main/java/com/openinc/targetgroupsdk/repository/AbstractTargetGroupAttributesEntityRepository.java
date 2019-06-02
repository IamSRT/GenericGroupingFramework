package com.openinc.targetgroupsdk.repository;

import com.google.common.collect.Maps;
import com.openinc.targetgroupsdk.businessObjects.TargetGroupAttributesEntity;

import java.util.Collection;
import java.util.List;

/**
 * Created by Sai Ravi Teja K on Friday 22, Mar 2019
 **/

public abstract class AbstractTargetGroupAttributesEntityRepository {

    protected String targetGroupType;

    public abstract List<TargetGroupAttributesEntity> createGroupingAttributes();

    public abstract List<TargetGroupAttributesEntity> createGroupingAttributes(Collection<String> entityIds);

    protected TargetGroupAttributesEntity getTargetGroupAttributes(String entityId){
        TargetGroupAttributesEntity targetGroupAttributesEntity = TargetGroupAttributesEntity.builder()
                .targetGroupEntityId(entityId)
                .type(targetGroupType)
                .attributeValueMap(Maps.newHashMap())
                .build();

        targetGroupAttributesEntity.addAttribute("groupingResult", false);
        return targetGroupAttributesEntity;
    }
}
