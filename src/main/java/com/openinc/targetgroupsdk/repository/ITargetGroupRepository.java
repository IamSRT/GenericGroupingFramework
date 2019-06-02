package com.openinc.targetgroupsdk.repository;

import com.openinc.targetgroupsdk.model.TargetGroup;

import java.util.Collection;
import java.util.List;

/**
 * Created by Sai Ravi Teja K on Tuesday 02, Apr 2019
 **/
public interface ITargetGroupRepository<T> {
    List<T> getAllTargetGroups();
    T getTargetGroupByGroupId(String groupId);
    List<T> batchGetTargetGroups(Collection<String> groupIds);
    void createOrUpdateTargetGroup(TargetGroup targetGroup, String updatedBy);
}
