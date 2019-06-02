package com.openinc.targetgroupsdk.mappers;

import com.openinc.targetgroupsdk.businessObjects.TargetGroupBO;
import com.openinc.targetgroupsdk.model.TargetGroup;
import org.mapstruct.Mapper;

/**
 * Created by Sai Ravi Teja K on Monday 25, Mar 2019
 **/

@Mapper
public interface TargetGroupMapper {
    TargetGroup targetGroupBoToModel(TargetGroupBO targetGroupBO);
    TargetGroupBO targetGroupModelToBo(TargetGroup targetGroup);
}
