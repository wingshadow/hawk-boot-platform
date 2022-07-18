package com.hawk.redis.stream.config;

import com.hawk.redis.stream.consumer.RedisConsumer;
import com.hawk.redis.stream.infc.MsgHandler;
import com.hawk.redis.stream.utils.RedisStreamOperator;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @program: hawk-boot-platform
 * @description: 初始化消费者配置
 * @author: zhb
 * @create: 2022-07-18 10:48
 */
@Component
public class ConsumerInitialized implements InitializingBean {

    @Resource
    private MsgHandler msgHandler;

    @Resource
    private RedisStreamOperator redisStreamOperator;

    @Resource
    private RedisStreamProperties redisStreamProperties;

    @Override
    public void afterPropertiesSet() {
        if (redisStreamProperties.isMultiEnable()) {
            for (int i = 0; i < redisStreamProperties.getMultiConsumerCount(); i++) {
                RedisConsumer consumer = new RedisConsumer();
                consumer.setRedisStreamOperator(redisStreamOperator);
                consumer.setMsgHandler(msgHandler);
                consumer.setRedisStreamProperties(redisStreamProperties);
                consumer.start();
            }
        } else {
            RedisConsumer consumer = new RedisConsumer();
            consumer.setRedisStreamOperator(redisStreamOperator);
            consumer.setMsgHandler(msgHandler);
            consumer.setRedisStreamProperties(redisStreamProperties);
            consumer.start();
        }
    }
}