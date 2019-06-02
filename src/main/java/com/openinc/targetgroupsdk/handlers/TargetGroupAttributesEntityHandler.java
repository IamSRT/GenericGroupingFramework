package com.openinc.targetgroupsdk.handlers;

import com.openinc.targetgroupsdk.businessObjects.TargetGroupAttributesEntity;
import com.openinc.targetgroupsdk.repository.AbstractTargetGroupAttributesEntityRepository;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Created by Sai Ravi Teja K on Thursday 21, Mar 2019
 **/

@Slf4j
public class TargetGroupAttributesEntityHandler {
    private AbstractTargetGroupAttributesEntityRepository abstractTargetGroupAttributesEntityRepository;
    private Executor executor;

    @Inject
    public TargetGroupAttributesEntityHandler(AbstractTargetGroupAttributesEntityRepository abstractTargetGroupAttributesEntityRepository,
                                              Executor executor){
        this.abstractTargetGroupAttributesEntityRepository = abstractTargetGroupAttributesEntityRepository;
        this.executor = executor;
    }


    public String createGroupAttributeEntitiesAsync(){
        try {
            CompletableFuture.supplyAsync(this::createGroupingAttributes, executor);
        } catch (Exception e){
            log.error("Exception occurred while creating group attributes for group type: {}", e);
            throw e;
        }
        return "Success";
    }

    public List<TargetGroupAttributesEntity> createGroupingAttributes() {
        return abstractTargetGroupAttributesEntityRepository.createGroupingAttributes();
    }

    public String createGroupAttributesEntityAsync(Collection<String> entityIds){
        try {
            CompletableFuture.supplyAsync(() -> createGroupingAttributes(entityIds), executor);
        } catch (Exception e){
            log.error("Exception occurred while creating group attributes for group type: {}", entityIds, e);
            throw e;
        }
        return "Success";
    }

    List<TargetGroupAttributesEntity> createGroupingAttributes(Collection<String> entityIds) {
        return abstractTargetGroupAttributesEntityRepository.createGroupingAttributes(entityIds);
    }
}
