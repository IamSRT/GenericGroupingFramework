package com.openinc.targetgroupsdk.handlers;

import com.google.common.collect.Lists;
import com.openinc.targetgroupsdk.repository.ITargetGroupMappingRepository;
import com.openinc.targetgroupsdk.businessObjects.TargetGroupBO;
import com.openinc.targetgroupsdk.enums.TargetGroupStatus;
import com.openinc.targetgroupsdk.events.TargetGroupUpdatedEvent;
import com.openinc.targetgroupsdk.model.TargetGroupMapping;
import com.openinc.targetgroupsdk.utils.EventStreamingUtil;
import com.openinc.targetgroupsdk.utils.TGUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Sai Ravi Teja K on Tuesday 26, Mar 2019
 **/

@Slf4j
public abstract class AbstractTargetGroupMappingHandler {
    protected ITargetGroupMappingRepository<TargetGroupMapping> iTargetGroupMappingRepository;

    public List<TargetGroupMapping> getActiveTargetGroupMappings(String entityId) {
        List<TargetGroupMapping> targetGroupMappings = iTargetGroupMappingRepository.getTargetGroupMappings(entityId);

        return targetGroupMappings.stream()
                .filter(tgm -> TargetGroupStatus.ACTIVE.equals((tgm.getStatus())))
                .collect(Collectors.toList());
    }

    public List<TargetGroupMapping> getExistingTGMappingsForEntityId(String entityId) {
        return iTargetGroupMappingRepository.getExistingTGMappingsForEntityId(entityId);
    }

    public List<TargetGroupMapping> getExistingTGMappingsForGroupId(String groupId) {
        return iTargetGroupMappingRepository.getExistingTGMappingsForGroupId(groupId);
    }

    public void createTargetGroupMappings(Collection<String> entityIds, TargetGroupBO targetGroupBO) {
        List<TargetGroupMapping> targetGroupMappings = new ArrayList<>();
        int i = 0;
        for (String entityId: entityIds) {
            i++;
            if (i%100 == 0) {
                createOrUpdateAndPublishTargetGroupMappings(targetGroupMappings);
                targetGroupMappings.clear();
            }

            TargetGroupMapping targetGroupMapping= new TargetGroupMapping();
            targetGroupMapping.setEntityId(entityId);
            targetGroupMapping.setGroupId(targetGroupBO.getGroupId());
            targetGroupMapping.setStatus(TargetGroupStatus.valueOf(targetGroupBO.getStatus()));
            targetGroupMappings.add(targetGroupMapping);
        }

        createOrUpdateAndPublishTargetGroupMappings(targetGroupMappings);

    }

    public void createTargetGroupMappings(Collection<String> targetGroupIds, String entityId, TargetGroupStatus status) {
        List<TargetGroupMapping> targetGroupMappings = Lists.newArrayList();

        int i = 0;
        for (String targetGroupId: targetGroupIds) {
            i++;

            if (i%100 == 0) {
                createOrUpdateAndPublishTargetGroupMappings(targetGroupMappings);
                targetGroupMappings.clear();
            }

            TargetGroupMapping targetGroupMapping= new TargetGroupMapping();
            targetGroupMapping.setEntityId(entityId);
            targetGroupMapping.setGroupId(targetGroupId);
            targetGroupMapping.setStatus(status);
            targetGroupMappings.add(targetGroupMapping);
        }

        createOrUpdateAndPublishTargetGroupMappings(targetGroupMappings);
    }

    public void updateTargetGroupMappings(List<TargetGroupMapping> targetGroupMappings, TargetGroupStatus status) {
        List<TargetGroupMapping> targetGroupMappingList = new ArrayList<>(targetGroupMappings);
        targetGroupMappingList = targetGroupMappingList.stream().map(tgm -> {tgm.setStatus(status); return tgm;})
                .collect(Collectors.toList());

        List<TargetGroupMapping> targetGroupMappingSubList = new ArrayList<>();
        int i = 0;
        for (TargetGroupMapping targetGroupMapping: targetGroupMappingList) {
            i++;
            if (i%100 == 0) {
                createOrUpdateAndPublishTargetGroupMappings(targetGroupMappingSubList);
                targetGroupMappingSubList.clear();
            }

            targetGroupMappingSubList.add(targetGroupMapping);
        }

        createOrUpdateAndPublishTargetGroupMappings(targetGroupMappingSubList);
    }

    private void createOrUpdateAndPublishTargetGroupMappings(List<TargetGroupMapping> targetGroupMappings){
        if(targetGroupMappings.isEmpty()) return;

        iTargetGroupMappingRepository.createOrUpdateTargetGroupMappings(targetGroupMappings, TGUtils.getUserId());
        publishTargetGroupUpdatedEvents(targetGroupMappings);
    }

    private void publishTargetGroupUpdatedEvents(Collection<TargetGroupMapping> targetGroupMappings) {
        if (targetGroupMappings == null || targetGroupMappings.size() == 0) {
            log.info("No events to publish as empty targetGroupMappings");
            return;
        }

        Map<String, TargetGroupUpdatedEvent> groupToEventMap = new HashMap<>();

        for(TargetGroupMapping targetGroupMapping: targetGroupMappings) {
            if(targetGroupMapping == null || StringUtils.isEmpty(targetGroupMapping.getEntityId())
                    || StringUtils.isEmpty(targetGroupMapping.getGroupId()) || targetGroupMapping.getStatus() == null )
                continue;

            String groupId = targetGroupMapping.getGroupId();
            TargetGroupUpdatedEvent targetGroupUpdatedEvent = groupToEventMap.getOrDefault(groupId, new TargetGroupUpdatedEvent(groupId));
            if (TargetGroupStatus.ACTIVE.equals(targetGroupMapping.getStatus()))
                targetGroupUpdatedEvent.getEntityIdsToActivate().add(targetGroupMapping.getEntityId());
            else
                targetGroupUpdatedEvent.getEntityIdsToDeactivate().add(targetGroupMapping.getEntityId());

            groupToEventMap.put(groupId, targetGroupUpdatedEvent);
        }

        for (Map.Entry<String, TargetGroupUpdatedEvent> entry : groupToEventMap.entrySet()) {
            EventStreamingUtil.publishEvent(getTargetGroupUpdatedEventTopic(), entry.getValue(), UUID.randomUUID().toString(),
                    entry.getValue().getGroupId());
        }
    }

    protected abstract String getTargetGroupUpdatedEventTopic();
}
