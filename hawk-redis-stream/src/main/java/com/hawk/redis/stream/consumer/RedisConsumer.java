package com.hawk.redis.stream.consumer;

import cn.hutool.core.exceptions.ExceptionUtil;
import com.hawk.redis.stream.config.RedisStreamProperties;
import com.hawk.redis.stream.infc.MsgHandler;
import com.hawk.redis.stream.utils.RedisStreamOperator;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;

/**
 * @program: springboot-stream-redis
 * @description:
 * @author: zhb
 * @create: 2022-06-20 09:23
 * <pre>
 *     多消费者模式
 * </pre>
 */
@Slf4j
@Data
public class RedisConsumer implements StreamListener,
        DisposableBean {

    private MsgHandler msgHandler;

    private Subscription subscription;

    private RedisStreamOperator redisStreamOperator;

    private RedisStreamProperties redisStreamProperties;

    private StreamMessageListenerContainer listenerContainer;

    @Override
    public void destroy() {
        if (subscription != null) {
            subscription.cancel();
        }
        if (listenerContainer != null) {
            listenerContainer.stop();
        }
    }

    public void start() {
        try {
            redisStreamOperator.createStreamConsumerGroup(redisStreamProperties.getTopic(), redisStreamProperties.getGroup());
            listenerContainer = redisStreamOperator.createStreamMessageListenerContainer();
            subscription = listenerContainer.receive(
                    Consumer.from(redisStreamProperties.getGroup(), redisStreamProperties.getConsumer()),
                    StreamOffset.create(redisStreamProperties.getTopic(), ReadOffset.lastConsumed()),
                    this
            );
            subscription.await(Duration.ofSeconds(1));
            listenerContainer.start();
        } catch (Exception e) {
            log.error("consumer start fail:{}", ExceptionUtil.stacktraceToString(e));
        }
    }

    @Override
    public void onMessage(Record record) {
        try {
            String value = (String) record.getValue();
            log.info("one record Id:{}", record.getId().getValue());
            msgHandler.handle(value);
        } catch (Exception e) {
            log.error("Exception:{}", ExceptionUtil.stacktraceToString(e));
            log.error("message handle fail recordId:{},value:{}", record.getId().getValue(),
                    record.getValue().toString());
        } finally {
            // 最后必须ack消息,否则pending队列内存溢出
            redisStreamOperator.delete(record);
            redisStreamOperator.ackStream(redisStreamProperties.getGroup(), record);
        }
    }
}