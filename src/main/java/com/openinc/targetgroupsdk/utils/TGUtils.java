package com.openinc.targetgroupsdk.utils;

import com.openinc.targetgroupsdk.businessObjects.TargetGroupAttributesEntity;
import com.openinc.targetgroupsdk.enums.TargetGroupPurpose;
import com.openinc.targetgroupsdk.exceptions.InvalidInputException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.jeasy.rules.mvel.MVELRule;

import java.util.Map;
import java.util.function.Function;

/**
 * Created by Sai Ravi Teja K on Tuesday 26, Mar 2019
 **/
public class TGUtils {
    private final static RulesEngine rulesEngine = new DefaultRulesEngine();

    public static  <T,U> U getOrDefault(T operand, U defaultValue, Function<T,U> function){
        if(operand == null) return defaultValue;
        return function.apply(operand);
    }

    public static String getUserId() {
        throw new NotImplementedException("Not implemented yet");
    }

    public static TargetGroupPurpose getTargetGroupPurpose(String purpose) throws InvalidInputException {
        TargetGroupPurpose groupPurpose = null;
        if (!StringUtils.isEmpty(purpose)) {
            try {
                groupPurpose = TargetGroupPurpose.valueOf(purpose);
            } catch (Exception e) {
                throw new InvalidInputException("Purpose parameter " + purpose + " is invalid");
            }
        }
        return groupPurpose;
    }

    public static Boolean doesAttributeEntityPassRule(TargetGroupAttributesEntity targetGroupAttributesEntity,
                                                      Rules rules) {

        // Fire rule for each of the target group attribute entity
        Facts facts = new Facts();
        targetGroupAttributesEntity.getAttributeValueMap()
                .put("groupingResult", false);
        facts.put(targetGroupAttributesEntity.getType().toLowerCase(), targetGroupAttributesEntity.getAttributeValueMap());

        rulesEngine.fire(rules, facts);

        Map<String, Object> result = facts.get(targetGroupAttributesEntity.getType().toLowerCase());
        return  Boolean.valueOf(result.get("groupingResult").toString());
    }

    public static Boolean doesAttributeEntityPassRule(TargetGroupAttributesEntity targetGroupAttributesEntity,
                                                      String ruleCondition) {
        //Create MVelRule
        MVELRule groupRule = new MVELRule()
                .name("groupRule")
                .description("groupRuleDescription")
                .priority(1)
                .when(ruleCondition)
                .then(targetGroupAttributesEntity.getType().toLowerCase() + ".groupingResult = true;");

        // create a rule set
        Rules rules = new Rules();
        rules.register(groupRule);

        // Fire rule for each of the target group attribute entity
        Facts facts = new Facts();
        targetGroupAttributesEntity.getAttributeValueMap()
                .put("groupingResult", false);
        facts.put(targetGroupAttributesEntity.getType().toLowerCase(), targetGroupAttributesEntity.getAttributeValueMap());

        rulesEngine.fire(rules, facts);

        Map<String, Object> result = facts.get(targetGroupAttributesEntity.getType().toLowerCase());
        return  Boolean.valueOf(result.get("groupingResult").toString());
    }
}
