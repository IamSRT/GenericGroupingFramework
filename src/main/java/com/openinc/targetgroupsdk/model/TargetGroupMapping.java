package com.openinc.targetgroupsdk.model;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.openinc.targetgroupsdk.enums.TargetGroupStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Created by Sai Ravi Teja K on Thursday 21, Mar 2019
 **/

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class TargetGroupMapping {

    @DynamoDBHashKey
    @DynamoDBIndexHashKey(globalSecondaryIndexName = "entityId-status-index")
    private String entityId;

    @DynamoDBRangeKey
    @DynamoDBIndexHashKey(globalSecondaryIndexName = "groupId-status-index")
    private String groupId;

    @DynamoDBIndexRangeKey(globalSecondaryIndexNames = {"entityId-status-index", "groupId-status-index"})
    @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.S)
    private TargetGroupStatus status;
}
