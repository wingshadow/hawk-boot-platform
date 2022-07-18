package com.hawk.redis.stream.infc;

/**
 * @program: hawk-boot-platform
 * @description: 消息处理接口
 * @author: zhb
 * @create: 2022-07-18 10:40
 */
public interface MsgHandler {
    /**
     * 消息处理方法
     * @param msg
     */
    void handle(String msg);
}