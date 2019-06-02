package com.openinc.targetgroupsdk.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTyped;
import com.openinc.targetgroupsdk.enums.TargetGroupPurpose;
import com.openinc.targetgroupsdk.enums.TargetGroupStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * Created by Sai Ravi Teja K on Thursday 21, Mar 2019
 **/

@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class TargetGroup {

    @DynamoDBHashKey
    private String groupId;

    @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.S)
    private String type;

    @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.S)
    private TargetGroupStatus status;

    @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.S)
    private TargetGroupPurpose purpose;

    private String name;
    private String description;
    private String owner;

    private Set<String> includeIds;
    private Set<String> excludeIds;

    private String includeConditionExpression;
    private String excludeConditionExpression;
}
