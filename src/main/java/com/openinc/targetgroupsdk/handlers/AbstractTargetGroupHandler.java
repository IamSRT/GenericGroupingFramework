package com.openinc.targetgroupsdk.handlers;

import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jcabi.aspects.RetryOnFailure;
import com.openinc.targetgroupsdk.businessObjects.TargetGroupAttributesEntity;
import com.openinc.targetgroupsdk.businessObjects.TargetGroupBO;
import com.openinc.targetgroupsdk.enums.TargetGroupPurpose;
import com.openinc.targetgroupsdk.enums.TargetGroupStatus;
import com.openinc.targetgroupsdk.events.IngestionJobRowUpdateEvent;
import com.openinc.targetgroupsdk.exceptions.InvalidInputException;
import com.openinc.targetgroupsdk.mappers.TargetGroupMapper;
import com.openinc.targetgroupsdk.model.TargetGroup;
import com.openinc.targetgroupsdk.model.TargetGroupMapping;
import com.openinc.targetgroupsdk.repository.ITargetGroupRepository;
import com.openinc.targetgroupsdk.request.TargetGroupAsyncRequest;
import com.openinc.targetgroupsdk.request.TargetGroupEntitiesUpdatedEvent;
import com.openinc.targetgroupsdk.request.UpdateEntitiesInTargetGroup;
import com.openinc.targetgroupsdk.response.TargetGroupValidityResponse;
import com.openinc.targetgroupsdk.utils.EventStreamingUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.mvel.MVELRule;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.openinc.targetgroupsdk.utils.TGUtils.*;

/**
 * Created by Sai Ravi Teja K on Monday 25, Mar 2019
 **/

@Slf4j
public abstract class AbstractTargetGroupHandler {
    private static final String DEFAULT_GROUP = "DEFAULT";
    private ITargetGroupRepository<TargetGroup> iTargetGroupRepository;
    private TargetGroupMapper targetGroupMapper;
    private TargetGroupAttributesEntityHandler targetGroupAttributesEntityHandler;
    private AbstractTargetGroupMappingHandler targetGroupMappingHandler;
    private Executor executor;

    public TargetGroupBO getTargetGroupByGroupId(String groupId) throws InvalidInputException {
        if (StringUtils.isEmpty(groupId)) throw new InvalidInputException("No GroupId found in request");


        TargetGroup targetGroup = iTargetGroupRepository.getTargetGroupByGroupId(groupId);
        if (targetGroup == null) {
            throw new InvalidInputException("No Group exists for id " + groupId);
        }
        return targetGroupMapper.targetGroupModelToBo(targetGroup);
    }

    public Set<String> getTargetGroupIdsForEntityId(String entityId, String purpose) throws InvalidInputException {
        if (StringUtils.isEmpty(entityId)) return Sets.newHashSet();

        TargetGroupPurpose groupPurpose = getTargetGroupPurpose(purpose);

        List<TargetGroupMapping> targetGroupMappings = targetGroupMappingHandler.getActiveTargetGroupMappings(entityId);

        if (targetGroupMappings.size() == 0) return Collections.singleton(DEFAULT_GROUP);

        Set<String> targetGroupIds = targetGroupMappings.stream()
                .map(TargetGroupMapping::getGroupId)
                .collect(Collectors.toSet());

        if (groupPurpose != null) {
            List<TargetGroup> targetGroups = iTargetGroupRepository.batchGetTargetGroups(targetGroupIds);

            targetGroupIds = targetGroups.stream()
                    .filter(targetGroup ->  groupPurpose.equals(targetGroup.getPurpose()))
                    .map(TargetGroup::getGroupId)
                    .collect(Collectors.toSet());
        }

        return targetGroupIds;
    }

    public List<TargetGroupBO> getTargetGroupDump(String purpose) throws InvalidInputException {
        TargetGroupPurpose groupPurpose = getTargetGroupPurpose(purpose);

        List<TargetGroup> allTargetGroups = iTargetGroupRepository.getAllTargetGroups();
        List<TargetGroup> targetGroupsToDump;

        if (groupPurpose != null) {
            targetGroupsToDump = allTargetGroups.stream()
                    .filter(tg -> tg.getPurpose().equals(groupPurpose))
                    .collect(Collectors.toList());
        } else {
            targetGroupsToDump =  new ArrayList<>(allTargetGroups);
        }

        return targetGroupsToDump.stream()
                .map(targetGroup -> targetGroupMapper.targetGroupModelToBo(targetGroup))
                .collect(Collectors.toList());
    }

    public Set<String> getTargetGroupFromAttributes(
            TargetGroupAttributesEntity targetGroupAttributesEntity, String purpose) throws InvalidInputException {
        if (targetGroupAttributesEntity == null) return new HashSet<>();

        TargetGroupPurpose groupPurpose = getTargetGroupPurpose(purpose);

        List<TargetGroup> targetGroups = iTargetGroupRepository.getAllTargetGroups();

        if (groupPurpose != null)
            targetGroups = targetGroups.stream().filter(tg -> tg.getStatus() != null && tg.getPurpose() != null)
                    .filter(tg -> tg.getStatus().equals(TargetGroupStatus.ACTIVE) && tg.getPurpose().equals(groupPurpose))
                    .collect(Collectors.toList());
        else
            targetGroups = targetGroups.stream().filter(tg -> tg.getStatus() != null)
                    .filter(tg -> tg.getStatus().equals(TargetGroupStatus.ACTIVE))
                    .collect(Collectors.toList());

        return targetGroups.stream()
                .filter(tg -> checkGroupMembership(targetGroupAttributesEntity, tg))
                .map(TargetGroup::getGroupId)
                .collect(Collectors.toSet());
    }

    private Set<TargetGroupAttributesEntity> filterTargetGroupAttributeEntities(String groupType,
                                                                                Collection<TargetGroupAttributesEntity> targetGroupEntries,
                                                                                String ruleCondition) {
        //Create MVelRule
        MVELRule groupRule = new MVELRule()
                .name("groupRule")
                .description("groupRuleDescription")
                .priority(1)
                .when(ruleCondition)
                .then(groupType.toLowerCase() + ".groupingResult = true;");

        // create a rule set
        Rules rules = new Rules();
        rules.register(groupRule);

        // check each attribute entity and the ones passing the rule
        return targetGroupEntries.stream().filter(e ->doesAttributeEntityPassRule(e,rules)).collect(Collectors.toSet());
    }

    public String createOrUpdateTargetGroupAsync(
            TargetGroupAsyncRequest targetGroupAsyncRequest, boolean isUpdateExisting) throws InvalidInputException {

        if (targetGroupAsyncRequest == null || targetGroupAsyncRequest.getTargetGroupBO() == null)
            throw  new InvalidInputException("Target Group Request cannot be null");

        if (StringUtils.isEmpty(targetGroupAsyncRequest.getJobId()))
            throw  new InvalidInputException("jobId cannot be null");

        if (StringUtils.isEmpty(targetGroupAsyncRequest.getRowId()))
            throw  new InvalidInputException("rowId cannot be null");

        CompletableFuture.supplyAsync(() -> createOrUpdateTargetGroupAndSendEvent(targetGroupAsyncRequest, isUpdateExisting), executor);
        return "Success";
    }

    private String createOrUpdateTargetGroupAndSendEvent(TargetGroupAsyncRequest targetGroupAsyncRequest, boolean isUpdateExisting) {
        log.info("Picked from executor - jobId: " + targetGroupAsyncRequest.getJobId() + " rowId: " + targetGroupAsyncRequest.getRowId());

        String msglog = "";
        String status;

        try {
            TargetGroupBO targetGroupBO;
            String suffix = "created";
            if (!isUpdateExisting)
                targetGroupBO = createTargetGroup(targetGroupAsyncRequest.getTargetGroupBO());
            else {
                targetGroupBO = updateTargetGroup(targetGroupAsyncRequest.getTargetGroupBO());
                suffix = "updated";
            }

            String groupId = targetGroupBO.getGroupId();
            msglog += groupId + " successfully " + suffix;
            status = "SUCCESS";
        } catch (Exception e) {
            if (!isUpdateExisting) {
                log.warn("Exception in creating target group ", e);
                msglog += "Exception in creating target group - " + e.getMessage();
            } else {
                log.warn("Exception in updating target group ", e);
                msglog += "Exception in updating target group - " + e.getMessage();
            }
            status = "FAILED";
        }

        //publish events
        log.info("Sending events for - jobId: " + targetGroupAsyncRequest.getJobId()+ " rowId: " + targetGroupAsyncRequest.getRowId());

        IngestionJobRowUpdateEvent ingestionJobRowUpdateEvent = IngestionJobRowUpdateEvent.builder()
                .jobId(targetGroupAsyncRequest.getJobId())
                .rowId(targetGroupAsyncRequest.getRowId())
                .rowExecutionId(targetGroupAsyncRequest.getRowExecutionId())
                .logToAppend(msglog)
                .status(status).build();

        return EventStreamingUtil.publishEvent(getIngestionTGRowUpdatedTopic(), ingestionJobRowUpdateEvent, UUID.randomUUID().toString(),
                ingestionJobRowUpdateEvent.getJobId());
    }

    public TargetGroupBO createTargetGroup(TargetGroupBO targetGroupBO) throws InvalidInputException {
        // Create Target Group entry
        // Find ids lying inside TargetGroup
        // Create Target Group Mappings
        if (targetGroupBO == null)
            throw  new InvalidInputException("TargetGroup Request cannot be null");

        if (!StringUtils.isEmpty(targetGroupBO.getGroupId()))
            throw  new InvalidInputException("Target Group Id should not be present in create request");

        populateIncludeExcludeEntityIds(targetGroupBO);
        Set<String> includeIds = targetGroupBO.getIncludeIds();
        targetGroupBO.setIncludeIds(Sets.newHashSet());

        targetGroupBO = createTargetGroupEntry(targetGroupBO);

        targetGroupMappingHandler.createTargetGroupMappings(includeIds, targetGroupBO);
        return targetGroupBO;
    }

    private void populateIncludeExcludeEntityIds(TargetGroupBO targetGroupBO){
        List<TargetGroupAttributesEntity> targetGroupAttributesEntities = targetGroupAttributesEntityHandler.createGroupingAttributes();
        Set<String> includeIds = getIncludedEntityIdsFromGroupDefinition(targetGroupBO, targetGroupAttributesEntities);
        Set<String> excludeIds = getExcludedIdsFromGroupDefinition(targetGroupBO, targetGroupAttributesEntities.stream()
                .filter(e -> includeIds.contains(e.getTargetGroupEntityId())).collect(Collectors.toSet()));
        includeIds.removeAll(excludeIds);
        targetGroupBO.setIncludeIds(includeIds);
        targetGroupBO.setExcludeIds(excludeIds);
    }

    private Set<String> getIncludedEntityIdsFromGroupDefinition(TargetGroupBO targetGroupBo,
                                                                Collection<TargetGroupAttributesEntity> targetGroupAttributesEntities) {
        // Calculate Included Entity ids
        Set<String> includedIds = new HashSet<>();
        if (targetGroupBo.getStatus().equals(TargetGroupStatus.INACTIVE.name())) return includedIds;

        if (targetGroupBo.getIncludeIds() != null && targetGroupBo.getIncludeIds().size() > 0)
            includedIds.addAll(targetGroupBo.getIncludeIds());

        if (!StringUtils.isEmpty(targetGroupBo.getIncludeConditionExpression())) {
            includedIds.addAll(filterTargetGroupAttributeEntities(targetGroupBo.getType(), targetGroupAttributesEntities,
                    targetGroupBo.getIncludeConditionExpression())
                    .stream()
                    .map(TargetGroupAttributesEntity::getTargetGroupEntityId)
                    .collect(Collectors.toSet()));
        }

        return includedIds;
    }

    private Set<String> getExcludedIdsFromGroupDefinition(TargetGroupBO targetGroupBo,
                                                          Collection<TargetGroupAttributesEntity> targetGroupAttributesEntities){
        // calculate excluded entity ids
        Set<String> excludedIds = new HashSet<>();
        if (targetGroupBo.getExcludeIds() != null && targetGroupBo.getExcludeIds().size() > 0)
            excludedIds.addAll(targetGroupBo.getExcludeIds());


        if (!StringUtils.isEmpty(targetGroupBo.getExcludeConditionExpression())) {
            excludedIds.addAll(filterTargetGroupAttributeEntities(targetGroupBo.getType(),
                    targetGroupAttributesEntities, targetGroupBo.getExcludeConditionExpression())
                    .stream()
                    .map(TargetGroupAttributesEntity::getTargetGroupEntityId)
                    .collect(Collectors.toSet()));
        }

        return excludedIds;
    }

    // Method to fix issues in any listing groups
    public List<TargetGroupBO> rectifyTargetGroupBos() throws InvalidInputException {
        List<TargetGroup> targetGroups = iTargetGroupRepository.getAllTargetGroups();
        List<TargetGroupBO> targetGroupBOS = Lists.newArrayList();
        int i = 0;
        for (TargetGroup targetGroup : targetGroups) {
            TargetGroupBO targetGroupBO = targetGroupMapper.targetGroupModelToBo(targetGroup);
            targetGroupBOS.add(updateTargetGroup(targetGroupBO));
            if(i%100 == 0){
                try {
                    //To Reduce chances of throttling at Taxonomy Service
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    log.error("Exception occurred while sleeping : ", e);
                }
            }

            i++;
        }

        return targetGroupBOS;
    }

    public TargetGroupBO updateTargetGroup(TargetGroupBO targetGroupBo) throws InvalidInputException {
        // Update Target Group Entry
        // Find existing entity ids lying inside target Group
        // find actual entity ids that should lie inside target Group
        // using diff , update Target Group Mappings as active or inactive , create new entries where required
        if (targetGroupBo == null)
            throw  new InvalidInputException("Target Group Request cannot be null");

        if (StringUtils.isEmpty(targetGroupBo.getGroupId()))
            throw  new InvalidInputException("Target group Id should be present in update request");

        targetGroupBo = updateTargetGroupEntry(targetGroupBo);
        Set<String> entityIdsInGroup = getEntityIdsFromGroupDefinition(targetGroupBo);

        List<TargetGroupMapping> targetGroupMappings = targetGroupMappingHandler.getExistingTGMappingsForGroupId(targetGroupBo.getGroupId());
        List<TargetGroupMapping> activeTargetGroupMappings = targetGroupMappings.stream()
                .filter(tgm -> TargetGroupStatus.ACTIVE.equals(tgm.getStatus()))
                .collect(Collectors.toList());

        List<TargetGroupMapping> inactiveTargetGroupMappings = targetGroupMappings.stream()
                .filter(tgm -> TargetGroupStatus.INACTIVE.equals(tgm.getStatus()))
                .collect(Collectors.toList());

        List<TargetGroupMapping> targetGroupMappingsToActivate = inactiveTargetGroupMappings.stream()
                .filter(tgm -> entityIdsInGroup.contains(tgm.getEntityId()))
                .collect(Collectors.toList());

        List<TargetGroupMapping> targetGroupMappingsToDeactivate = activeTargetGroupMappings.stream()
                .filter(tgm -> !entityIdsInGroup.contains(tgm.getEntityId()))
                .collect(Collectors.toList());

        Set<String> existingDbEntityIds = targetGroupMappings.stream()
                .map(TargetGroupMapping::getEntityId)
                .collect(Collectors.toSet());

        Set<String> targetGroupingMappingToCreate = entityIdsInGroup.stream()
                .filter(id -> !existingDbEntityIds.contains(id))
                .collect(Collectors.toSet());

        targetGroupMappingHandler.updateTargetGroupMappings(targetGroupMappingsToActivate, TargetGroupStatus.ACTIVE);
        targetGroupMappingHandler.updateTargetGroupMappings(targetGroupMappingsToDeactivate, TargetGroupStatus.INACTIVE);
        targetGroupMappingHandler.createTargetGroupMappings(targetGroupingMappingToCreate, targetGroupBo);
        targetGroupBo.setIncludeIds(entityIdsInGroup);
        return targetGroupBo;

    }

    public TargetGroupBO updateEntitiesInTargetGroup(UpdateEntitiesInTargetGroup updateEntitiesInTargetGroup) throws InvalidInputException {
        if (updateEntitiesInTargetGroup == null)
            return null;

        if (StringUtils.isEmpty(updateEntitiesInTargetGroup.getGroupId()))
            throw new InvalidInputException("Group id cannot be empty for update.");

        TargetGroupBO targetGroupBO = getTargetGroupByGroupId(updateEntitiesInTargetGroup.getGroupId());
        Set<String> idsToAdd = getOrDefault(updateEntitiesInTargetGroup.getEntityIdsToAdd(), Sets.newHashSet()) ;
        Set<String> idsToRemove = getOrDefault(updateEntitiesInTargetGroup.getEntityIdsToRemove(), Sets.newHashSet());
        Set<String> includedIds = getOrDefault(targetGroupBO.getIncludeIds(), Sets.newHashSet());
        Set<String> excludedIds = getOrDefault(targetGroupBO.getExcludeIds(), Sets.newHashSet());

        includedIds.addAll(idsToAdd);
        excludedIds.removeAll(idsToRemove);
        includedIds.removeAll(idsToRemove);
        excludedIds.addAll(idsToRemove);

        targetGroupBO.setIncludeIds(includedIds);
        targetGroupBO.setExcludeIds(excludedIds);
        iTargetGroupRepository.createOrUpdateTargetGroup(targetGroupMapper.targetGroupBoToModel(targetGroupBO), getUserId());
        return targetGroupBO;
    }

    public void onGroupAttributeEntityUpdate(String entityId, String type){
        TargetGroupEntitiesUpdatedEvent targetGroupEntitiesUpdatedEvent =
                TargetGroupEntitiesUpdatedEvent.builder().entityIdsUpdated(Sets.newHashSet(entityId)).build();
        onGroupAttributeEntityUpdate(targetGroupEntitiesUpdatedEvent, type);
    }

    public void onGroupAttributeEntityUpdate(List<String> entityIds, String type){
        TargetGroupEntitiesUpdatedEvent targetGroupEntitiesUpdatedEvent =
                TargetGroupEntitiesUpdatedEvent.builder().entityIdsUpdated(Sets.newHashSet(entityIds)).build();
        onGroupAttributeEntityUpdate(targetGroupEntitiesUpdatedEvent, type);
    }

    public void onGroupAttributeEntityUpdate(TargetGroupEntitiesUpdatedEvent entitiesUpdatedEvent, String type) {
        if (entitiesUpdatedEvent == null || entitiesUpdatedEvent.getEntityIdsUpdated() == null || entitiesUpdatedEvent.getEntityIdsUpdated().size() == 0)
            return;

        Set<String> entityIds = entitiesUpdatedEvent.getEntityIdsUpdated();
        List<TargetGroupAttributesEntity> targetGroupAttributesEntities = targetGroupAttributesEntityHandler.createGroupingAttributes(entityIds);
        List<TargetGroup> targetGroupDump = iTargetGroupRepository.getAllTargetGroups();

        for (TargetGroupAttributesEntity entity: targetGroupAttributesEntities) {
            Set<String> validTargetGroupIds = getValidTargetGroupsForEntity(entity, targetGroupDump);
            List<TargetGroupMapping> targetGroupMappings = targetGroupMappingHandler.getExistingTGMappingsForEntityId(entity.getTargetGroupEntityId());

            List<TargetGroupMapping> activeTargetGroupMappings = targetGroupMappings.stream()
                    .filter(tgm -> TargetGroupStatus.ACTIVE.equals(tgm.getStatus()))
                    .collect(Collectors.toList());

            List<TargetGroupMapping> inactiveTargetGroupMappings = targetGroupMappings.stream()
                    .filter(tgm -> TargetGroupStatus.INACTIVE.equals(tgm.getStatus()))
                    .collect(Collectors.toList());

            List<TargetGroupMapping> targetGroupMappingsToActivate = inactiveTargetGroupMappings.stream()
                    .filter(tgm -> validTargetGroupIds.contains(tgm.getGroupId()))
                    .collect(Collectors.toList());

            List<TargetGroupMapping> targetGroupMappingsToDeactivate = activeTargetGroupMappings.stream()
                    .filter(tgm -> !validTargetGroupIds.contains(tgm.getGroupId()))
                    .collect(Collectors.toList());

            Set<String> existingDbGroupIds = targetGroupMappings.stream()
                    .map(TargetGroupMapping::getGroupId)
                    .collect(Collectors.toSet());

            Set<String> groupMappingsToCreate = validTargetGroupIds.stream()
                    .filter(groupId -> !existingDbGroupIds.contains(groupId))
                    .collect(Collectors.toSet());

            targetGroupMappingHandler.updateTargetGroupMappings(targetGroupMappingsToActivate, TargetGroupStatus.ACTIVE);
            targetGroupMappingHandler.updateTargetGroupMappings(targetGroupMappingsToDeactivate, TargetGroupStatus.INACTIVE);
            targetGroupMappingHandler.createTargetGroupMappings(groupMappingsToCreate, entity.getTargetGroupEntityId(), TargetGroupStatus.ACTIVE);
        }
    }

    public TargetGroupValidityResponse isGroupIdValid(String groupId, String purpose) {
        TargetGroupValidityResponse targetGroupValidityResponse = new TargetGroupValidityResponse();
        if (StringUtils.isEmpty(groupId)) {
            targetGroupValidityResponse.setValid(false);
            targetGroupValidityResponse.setErrorMessage("Group Id cannot be empty");
            return targetGroupValidityResponse;
        }

        TargetGroup targetGroup = iTargetGroupRepository.getTargetGroupByGroupId(groupId);

        if (targetGroup == null) {
            targetGroupValidityResponse.setValid(false);
            targetGroupValidityResponse.setErrorMessage("No Target group for id " + groupId);
            return targetGroupValidityResponse;
        }

        if (!StringUtils.isEmpty(purpose)) {
            TargetGroupPurpose groupPurpose;
            try {
                groupPurpose = TargetGroupPurpose.valueOf(purpose);
            } catch (Exception e) {
                targetGroupValidityResponse.setValid(false);
                targetGroupValidityResponse.setErrorMessage("No Purpose configured with " + purpose);
                return targetGroupValidityResponse;
            }

            if(!groupPurpose.equals(targetGroup.getPurpose())) {
                targetGroupValidityResponse.setValid(false);
                targetGroupValidityResponse.setErrorMessage("Given purpose " + purpose + " does not match with the found group" +
                        " purpose " + targetGroup.getPurpose());
                return targetGroupValidityResponse;
            }
        }

        targetGroupValidityResponse.setValid(true);
        return targetGroupValidityResponse;
    }

    public Set<String> getActiveEntityIdsInGroup(String groupId) {
        Set<String> entityIds = Sets.newHashSet();

        if (StringUtils.isEmpty(groupId)) {
            log.warn("group id is empty return empty set");
            return entityIds;
        }

        return targetGroupMappingHandler.getExistingTGMappingsForGroupId(groupId).stream()
                .filter(tgm -> TargetGroupStatus.ACTIVE.equals(tgm.getStatus()))
                .map(TargetGroupMapping::getEntityId)
                .collect(Collectors.toSet());
    }

    private Set<String> getEntityIdsFromGroupDefinition(TargetGroupBO targetGroupBo) {
        // Calculate Included Entity ids
        Set<String> includedIds = new HashSet<>();
        if (targetGroupBo.getStatus().equals(TargetGroupStatus.INACTIVE.name())) return includedIds;

        if (targetGroupBo.getIncludeIds() != null && targetGroupBo.getIncludeIds().size() > 0)
            includedIds.addAll(targetGroupBo.getIncludeIds());

        List<TargetGroupAttributesEntity> targetGroupAttributesEntities = new ArrayList<>();
        if (!StringUtils.isEmpty(targetGroupBo.getIncludeConditionExpression())) {
            targetGroupAttributesEntities = new ArrayList<>(targetGroupAttributesEntityHandler.createGroupingAttributes());
            includedIds.addAll(filterTargetGroupAttributeEntities(targetGroupBo.getType(), targetGroupAttributesEntities,
                    targetGroupBo.getIncludeConditionExpression())
                    .stream()
                    .map(TargetGroupAttributesEntity::getTargetGroupEntityId)
                    .collect(Collectors.toSet()));
        }

        // calculate excluded entity ids
        Set<String> excludedIds = new HashSet<>();
        if (targetGroupBo.getExcludeIds() != null && targetGroupBo.getExcludeIds().size() > 0)
            excludedIds.addAll(targetGroupBo.getExcludeIds());


        if (!StringUtils.isEmpty(targetGroupBo.getExcludeConditionExpression())) {
            Set<TargetGroupAttributesEntity> targetGroupAttributesEntitySet = targetGroupAttributesEntities.stream()
                    .filter(tgae -> includedIds.contains(tgae.getTargetGroupEntityId()))
                    .collect(Collectors.toSet());

            excludedIds.addAll(filterTargetGroupAttributeEntities(targetGroupBo.getType(),
                    targetGroupAttributesEntitySet, targetGroupBo.getExcludeConditionExpression())
                    .stream()
                    .map(TargetGroupAttributesEntity::getTargetGroupEntityId)
                    .collect(Collectors.toSet()));
        }

        includedIds.removeAll(excludedIds);
        return includedIds;
    }

    private Set<String> getValidTargetGroupsForEntity(TargetGroupAttributesEntity entity, List<TargetGroup> targetGroups) {
        Set<String> validGrpIds = new HashSet<>();
        if (StringUtils.isEmpty(entity.getTargetGroupEntityId()))
            return validGrpIds;

        return targetGroups.stream()
                .filter(tg -> checkGroupMembership(entity, tg))
                .map(TargetGroup::getGroupId)
                .collect(Collectors.toSet());
    }

    private Set<String> getValidTargetGroupsForEntity(String entityId, List<TargetGroup> targetGroups) {
        Set<String> validGrpIds = new HashSet<>();
        if (StringUtils.isEmpty(entityId))
            return validGrpIds;

        return targetGroups.stream()
                .filter(tg -> createAndCheckAttributeEntityMembershipOfTargetGroup(entityId, tg))
                .map(TargetGroup::getGroupId)
                .collect(Collectors.toSet());
    }

    /**
     * Creates Attribute Entity from original entities
     * @param entityId entity id
     * @param targetGroup target to check membership
     * @return boolean is member or not
     */
    private boolean createAndCheckAttributeEntityMembershipOfTargetGroup(String entityId, TargetGroup targetGroup) {
        if (StringUtils.isEmpty(entityId) || targetGroup == null)
            return false;

        if (!TargetGroupStatus.ACTIVE.equals(targetGroup.getStatus()))
            return false;

        Optional<TargetGroupAttributesEntity> targetGroupAttributesEntity =
                targetGroupAttributesEntityHandler.createGroupingAttributes(Lists.newArrayList(entityId)).stream().findFirst();
        return targetGroupAttributesEntity.filter(tga -> checkGroupMembership(tga, targetGroup)).isPresent();
    }

    /**
     * Check if the given TG Attribute Entity is a member of group or not
     * @param targetGroupAttributesEntity target group attribute entity
     * @param targetGroup target group
     * @return is member or not
     */
    private boolean checkGroupMembership(TargetGroupAttributesEntity targetGroupAttributesEntity, TargetGroup targetGroup) {
        if (targetGroup == null || !TargetGroupStatus.ACTIVE.equals(targetGroup.getStatus()))
            return false;

        Set<String> includeIds = getOrDefault(targetGroup.getIncludeIds(), Sets.newHashSet());
        Set<String> excludedIds = getOrDefault(targetGroup.getExcludeIds(), Sets.newHashSet());

        boolean isIncluded = false;

        // include condition check
        if (includeIds.contains(getOrDefault(targetGroupAttributesEntity.getTargetGroupEntityId(), "")))
            isIncluded = true;

        if (!isIncluded && !StringUtils.isEmpty(targetGroup.getIncludeConditionExpression()))
            isIncluded = doesAttributeEntityPassRule(targetGroupAttributesEntity, targetGroup.getIncludeConditionExpression());

        // exclude condition check
        if (isIncluded && excludedIds.contains(getOrDefault(targetGroupAttributesEntity.getTargetGroupEntityId(), "")))
            isIncluded = false;


        if (isIncluded && !StringUtils.isEmpty(targetGroup.getExcludeConditionExpression()))
            if(doesAttributeEntityPassRule(targetGroupAttributesEntity, targetGroup.getExcludeConditionExpression()))
                isIncluded = false;

        return isIncluded;
    }

    @RetryOnFailure(attempts = 5, delay = 1, unit = TimeUnit.SECONDS, types = {ConditionalCheckFailedException.class})
    private TargetGroupBO createTargetGroupEntry(TargetGroupBO targetGroupBO) {
        String groupId = generateId();
        if (targetGroupBO.getIncludeIds() != null && targetGroupBO.getIncludeIds().size() == 0)
            targetGroupBO.setIncludeIds(null);

        if (targetGroupBO.getExcludeIds() != null && targetGroupBO.getExcludeIds().size() == 0)
            targetGroupBO.setExcludeIds(null);

        TargetGroup targetGroup = targetGroupMapper.targetGroupBoToModel(targetGroupBO);
        targetGroup.setGroupId(groupId);
        iTargetGroupRepository.createOrUpdateTargetGroup(targetGroup, getUserId());
        return targetGroupMapper.targetGroupModelToBo(targetGroup);
    }

    @RetryOnFailure(attempts = 5, delay = 1, unit = TimeUnit.SECONDS, types = {ConditionalCheckFailedException.class})
    private TargetGroupBO updateTargetGroupEntry(TargetGroupBO targetGroupBO) throws InvalidInputException {
        TargetGroup targetGroup = iTargetGroupRepository.getTargetGroupByGroupId(targetGroupBO.getGroupId());

        if (targetGroup == null)
            throw  new InvalidInputException("No db entry present for id " + targetGroupBO.getGroupId());

        if (targetGroupBO.getIncludeIds() != null && targetGroupBO.getIncludeIds().size() == 0)
            targetGroupBO.setIncludeIds(null);

        if (targetGroupBO.getExcludeIds() != null && targetGroupBO.getExcludeIds().size() == 0)
            targetGroupBO.setExcludeIds(null);

        targetGroup.setStatus(TargetGroupStatus.valueOf(targetGroupBO.getStatus()));
        targetGroup.setIncludeConditionExpression(targetGroupBO.getIncludeConditionExpression());
        targetGroup.setIncludeIds(targetGroupBO.getIncludeIds());
        targetGroup.setExcludeConditionExpression(targetGroupBO.getExcludeConditionExpression());
        targetGroup.setExcludeIds(targetGroupBO.getExcludeIds());
        targetGroup.setName(targetGroupBO.getName());
        targetGroup.setDescription(targetGroupBO.getDescription());
        targetGroup.setOwner(targetGroupBO.getOwner());
        iTargetGroupRepository.createOrUpdateTargetGroup(targetGroup, getUserId());

        return targetGroupMapper.targetGroupModelToBo(targetGroup);
    }

    private <T> T getOrDefault(T val, T defaultVal){
        return val == null ? defaultVal : val;
    }

    protected abstract String getIngestionTGRowUpdatedTopic();

    protected abstract String generateId();
}
