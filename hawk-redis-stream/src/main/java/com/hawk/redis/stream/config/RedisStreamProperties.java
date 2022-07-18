package com.hawk.redis.stream.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @program: hawk-boot-platform
 * @description:
 * @author: zhb
 * @create: 2022-07-18 10:42
 */
@Data
@Component
@ConfigurationProperties(prefix = "redis.mq")
public class RedisStreamProperties {
    private String topic;
    private String group;
    private String consumer;
    private boolean multiEnable;
    private int multiConsumerCount;
}