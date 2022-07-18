package com.hawk.redis.stream.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.Resource;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @program: hawk-boot-platform
 * @description:
 * @author: zhb
 * @create: 2022-07-18 13:13
 */
@EnableAsync
@Configuration
public class ThreadPoolTaskExecutorConfiguration {

    @Resource
    private RedisStreamProperties redisStreamProperties;

    @Bean(initMethod = "initialize", destroyMethod = "destroy")
    public ThreadPoolTaskExecutor consumerPoolTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(redisStreamProperties.getMultiConsumerCount());
        threadPoolTaskExecutor.setMaxPoolSize(2 * redisStreamProperties.getMultiConsumerCount());
        threadPoolTaskExecutor.setQueueCapacity(200);
        threadPoolTaskExecutor.setKeepAliveSeconds(30);
        threadPoolTaskExecutor.setThreadNamePrefix("team-consumer-");
        threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        threadPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        return threadPoolTaskExecutor;
    }

    @Bean(initMethod = "initialize", destroyMethod = "destroy")
    public ThreadPoolTaskExecutor sendPoolTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(4);
        threadPoolTaskExecutor.setMaxPoolSize(2 * 4);
        threadPoolTaskExecutor.setThreadNamePrefix("team-producer-");
        return threadPoolTaskExecutor;
    }
}