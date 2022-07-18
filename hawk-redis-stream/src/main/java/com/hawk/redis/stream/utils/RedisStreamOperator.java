package com.hawk.redis.stream.utils;

import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/**
 * @program: hawk-boot-platform
 * @description: stream操作工具类
 * @author: zhb
 * @create: 2022-07-18 10:50
 */
@Component
public class RedisStreamOperator {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ThreadPoolTaskExecutor consumerPoolTaskExecutor;

    public Object getRedisValue(String key, String field) {
        return this.stringRedisTemplate.opsForHash().get(key, field);
    }

    public long increaseRedisValue(String key, String field) {
        return stringRedisTemplate.opsForHash().increment(key, field, 1);
    }

    public void ackStream(String consumerGroupName, Record record) {
        stringRedisTemplate.opsForStream().acknowledge(consumerGroupName, record);
    }

    /**
     * 声明更换消费者
     *
     * @param pendingMessage
     * @param consumerName
     */
    public void claimStream(PendingMessage pendingMessage, String consumerName) {
        RedisAsyncCommands commands = (RedisAsyncCommands) stringRedisTemplate
                .getConnectionFactory().getConnection().getNativeConnection();

        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                .add(pendingMessage.getIdAsString())
                .add(pendingMessage.getGroupName())
                .add(consumerName)
                .add("20")
                .add(pendingMessage.getIdAsString());
        commands.dispatch(CommandType.XCLAIM, new StatusOutput(StringCodec.UTF8), args);
    }

    /**
     * 根据消息ID获取消息
     *
     * @param streamKey
     * @param id
     * @return
     */
    public MapRecord<String, Object, Object> findStreamMessageById(String streamKey, String id) {
        List<MapRecord<String, Object, Object>> mapRecordList = this.findStreamMessageByRange(streamKey, id, id);
        if (mapRecordList.isEmpty()) {
            return null;
        }
        return mapRecordList.get(0);
    }

    /**
     * 获取区间内消息内容
     *
     * @param streamKey
     * @param startId
     * @param endId
     * @return
     */
    public List<MapRecord<String, Object, Object>> findStreamMessageByRange(String streamKey, String startId,
                                                                            String endId) {
        return this.stringRedisTemplate.opsForStream().range(streamKey, Range.closed(startId, endId));
    }

    /**
     * 创建消费组
     *
     * @param streamKey
     * @param consumerGroupName
     */
    public void createStreamConsumerGroup(String streamKey, String consumerGroupName) {
        // if stream is not exist, create stream and consumer group of it
        if (Boolean.FALSE.equals(stringRedisTemplate.hasKey(streamKey))) {
            RedisAsyncCommands commands = (RedisAsyncCommands) this.stringRedisTemplate
                    .getConnectionFactory()
                    .getConnection()
                    .getNativeConnection();

            // 0 偏移量
            // MKSTREAM这个参数会判断stream是否存在,当创建消费组的键值对不存在时，则会创建一个新的消费组
            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .add(CommandKeyword.CREATE)
                    .add(streamKey)
                    .add(consumerGroupName)
                    .add("0")
                    .add("MKSTREAM");


            commands.dispatch(CommandType.XGROUP, new StatusOutput(StringCodec.UTF8), args);
        } else {
            // stream is exist, create consumerGroup if is not exist
            if (!isStreamConsumerGroupExist(streamKey, consumerGroupName)) {
                stringRedisTemplate.opsForStream().createGroup(streamKey, ReadOffset.from("0"), consumerGroupName);
            }
        }
    }

    /**
     * 获取已消费但无应答消息
     *
     * @param streamKey
     * @param consumerGroupName
     * @param consumerName
     * @return
     */
    public PendingMessages findStreamPendingMessages(String streamKey, String consumerGroupName, String consumerName) {
        return stringRedisTemplate.opsForStream()
                .pending(streamKey, Consumer.from(consumerGroupName, consumerName), Range.unbounded(), 100L);
    }

    /**
     * 判断消费者存在
     *
     * @param streamKey
     * @param consumerGroupName
     * @return
     */
    public boolean isStreamConsumerGroupExist(String streamKey, String consumerGroupName) {
        Iterator<StreamInfo.XInfoGroup> iterator = stringRedisTemplate
                .opsForStream().groups(streamKey).stream().iterator();
        while (iterator.hasNext()) {
            StreamInfo.XInfoGroup xInfoGroup = iterator.next();
            if (xInfoGroup.groupName().equals(consumerGroupName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 创建消费者容器
     *
     * @return
     */
    public StreamMessageListenerContainer createStreamMessageListenerContainer() {
        return StreamMessageListenerContainer.create(
                stringRedisTemplate.getConnectionFactory(),
                StreamMessageListenerContainer
                        .StreamMessageListenerContainerOptions.builder()
                        .hashKeySerializer(new StringRedisSerializer())
                        .hashValueSerializer(new StringRedisSerializer())
                        .pollTimeout(Duration.ZERO)
                        .batchSize(1)
                        .targetType(String.class)
                        .executor(consumerPoolTaskExecutor)
                        .build()
        );
    }

    /**
     * 删除消息
     *
     * @param record
     */
    public void delete(Record record) {
        stringRedisTemplate.opsForStream().delete(record);
    }

    /**
     * 根据消息ID删除消息
     *
     * @param topic
     * @param recordId
     */
    public void delete(String topic, RecordId recordId) {
        stringRedisTemplate.opsForStream().delete(topic, recordId);
    }
}