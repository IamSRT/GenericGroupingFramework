package com.openinc.targetgroupsdk.repository;

/**
 * Created by Sai Ravi Teja K on Monday 01, Apr 2019
 **/

import com.openinc.targetgroupsdk.model.TargetGroupMapping;

import java.util.List;

public interface ITargetGroupMappingRepository<T> {
    List<T> getTargetGroupMappings(String entityId);
    List<T> getExistingTGMappingsForEntityId(String entityId);
    List<T> getExistingTGMappingsForGroupId(String groupId);
    void createOrUpdateTargetGroupMappings(List<TargetGroupMapping> targetGroupMappings, String updatedBy);
}
