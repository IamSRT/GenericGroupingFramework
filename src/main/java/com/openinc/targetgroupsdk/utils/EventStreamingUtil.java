package com.openinc.targetgroupsdk.utils;

import com.jcabi.aspects.RetryOnFailure;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class EventStreamingUtil {

    @RetryOnFailure(attempts = 3, delay = 1, unit = TimeUnit.SECONDS, types = {Exception.class})
    public static String publishEvent(String topic, Object message, String idempotencyKey, String groupKey) {
        try {
            //publish message
            //return success message
            return "Success";

        } catch (Exception e) {
            log.warn("Message sending to failed topic {} message {} idempotencyKey {} groupKey {}",
                    topic, message, idempotencyKey, groupKey, e);
            throw new RuntimeException(e);
        }
    }
}
