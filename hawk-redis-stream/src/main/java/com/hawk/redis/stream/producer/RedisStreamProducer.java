package com.hawk.redis.stream.producer;

import com.hawk.redis.stream.config.RedisStreamProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @program: hawk-boot-platform
 * @description:
 * @author: zhb
 * @create: 2022-07-18 13:16
 */
@Slf4j
@Component
public class RedisStreamProducer {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedisStreamProperties redisStreamProperties;

    public RecordId send(String msg) {
        ObjectRecord<String, String> record =
                StreamRecords.newRecord().ofObject(msg).withStreamKey(redisStreamProperties.getTopic());
        RecordId recordId = stringRedisTemplate.opsForStream().add(record);
        return recordId;
    }
}