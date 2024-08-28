/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.message;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable {
    private static final long serialVersionUID = 8445773977080406428L;

    /**
     * topic
     */
    private String topic;
    /**
     * flag：默认是 0
     */
    private int flag;
    /**
     * 属性对映：
     * org.apache.rocketmq.spring.support.RocketMQHeaders
     *
     * 属性包括
     * TAGS:
     * KEYS:
     * WAIT：是否等待存储完成，默认是 true
     * DELAY：延迟等级，0 无延迟，大于 0才设置这个属性
     *
     * UNIQ_KEY：消息id
     * TRAN_MSG：是否是事务消息
     * MSG_REGION：消息区域，默认  DefaultRegion
     * TRACE_ON ：消息轨迹开关
     * __SHARDINGKEY： 标识是否顺序消息
     */
    private Map<String, String> properties;
    /**
     * 消息体
     * 消息大小 > 4M，抛出异常
     * 消息大小 > 4K，启动压缩
     */
    private byte[] body;

    private String transactionId;

    public Message() {
    }

    public Message(String topic, byte[] body) {
        this(topic, "", "", 0, body, true);
    }

    public Message(String topic, String tags, String keys, int flag, byte[] body, boolean waitStoreMsgOK) {
        this.topic = topic;
        this.flag = flag;
        this.body = body;

        if (tags != null && tags.length() > 0) {
            this.setTags(tags);
        }

        /**
         * keys 属性十分重要：
         * 消息路由：生产者端的作用
         *          当生产者发送消息时，可以通过设置 keys 值来控制消息被路由到哪些队列。
         *          这通常通过哈希算法实现，以确保具有相同 keys 的消息会被发送到相同的队列中。
         *          例如，假设你有一个订单系统，并且希望所有与特定订单 ID 相关的消息都被发送到同一个队列中进行处理。
         * 消息过滤：消费者端的作用
         *          消费者可以通过设置消息过滤器来只接收具有特定 keys 的消息
         *          例如，你可能有一个消费者只关心与特定用户 ID 关联的消息。在这种情况下，你可以为这些消息设置用户 ID 作为 keys，并配置消费者只消费具有这些 keys 的消息。
         *
         * compactionSchedule 定时压缩任务中也有使用到 keys 作为 offsetMap 的key
         */
        if (keys != null && keys.length() > 0) {
            this.setKeys(keys);
        }

        /**
         * 在 FlushDiskType.SYNC_FLUSH 同步刷盘下，waitStoreMsgOK 属性会影响 GroupCommitService 处理消息的 刷盘方式
         * WAIT 属性设置为 true，会创建 GroupCommitRequest 请求放入缓存，等待 GroupCommitService 定时扫描请求进行数据落盘
         * WAIT 属性设置为 false，会马上唤醒 GroupCommitService 线程，进行数据实时落盘
         */
        this.setWaitStoreMsgOK(waitStoreMsgOK);
    }

    /**
     * 在 使用 rocketMqTemplate.send(D destination, Message<?> message）方法
     * 消息转换为rocketmq消息的时候，创建对象
     */
    public Message(String topic, String tags, byte[] body) {
        this(topic, tags, "", 0, body, true);
    }

    public Message(String topic, String tags, String keys, byte[] body) {
        this(topic, tags, keys, 0, body, true);
    }

    /**
     *  创建Message对象会获取spring message header 中的 keys 属性，不存在还会尝试获取 rocketmq_keys 属性
     */
    public void setKeys(String keys) {
        this.putProperty(MessageConst.PROPERTY_KEYS, keys);
    }

    void putProperty(final String name, final String value) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }

        this.properties.put(name, value);
    }

    void clearProperty(final String name) {
        if (null != this.properties) {
            this.properties.remove(name);
        }
    }

    public void putUserProperty(final String name, final String value) {
        if (MessageConst.STRING_HASH_SET.contains(name)) {
            throw new RuntimeException(String.format(
                "The Property<%s> is used by system, input another please", name));
        }

        if (value == null || value.trim().isEmpty()
            || name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException(
                "The name or value of property can not be null or blank string!"
            );
        }

        this.putProperty(name, value);
    }

    public String getUserProperty(final String name) {
        return this.getProperty(name);
    }

    public String getProperty(final String name) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }

        return this.properties.get(name);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTags() {
        return this.getProperty(MessageConst.PROPERTY_TAGS);
    }

    public void setTags(String tags) {
        this.putProperty(MessageConst.PROPERTY_TAGS, tags);
    }

    public String getKeys() {
        return this.getProperty(MessageConst.PROPERTY_KEYS);
    }

    public void setKeys(Collection<String> keyCollection) {
        String keys = String.join(MessageConst.KEY_SEPARATOR, keyCollection);

        this.setKeys(keys);
    }

    public int getDelayTimeLevel() {
        String t = this.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        if (t != null) {
            return Integer.parseInt(t);
        }

        return 0;
    }

    public void setDelayTimeLevel(int level) {
        this.putProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL, String.valueOf(level));
    }

    /**
     * WAIT 属性
     * 为空默认为 需要等待
     * @return
     */
    public boolean isWaitStoreMsgOK() {
        String result = this.getProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
        if (null == result) {
            return true;
        }

        return Boolean.parseBoolean(result);
    }

    public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
        this.putProperty(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, Boolean.toString(waitStoreMsgOK));
    }

    public void setInstanceId(String instanceId) {
        this.putProperty(MessageConst.PROPERTY_INSTANCE_ID, instanceId);
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getBuyerId() {
        return getProperty(MessageConst.PROPERTY_BUYER_ID);
    }

    public void setBuyerId(String buyerId) {
        putProperty(MessageConst.PROPERTY_BUYER_ID, buyerId);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String toString() {
        return "Message{" +
            "topic='" + topic + '\'' +
            ", flag=" + flag +
            ", properties=" + properties +
            ", body=" + Arrays.toString(body) +
            ", transactionId='" + transactionId + '\'' +
            '}';
    }

    public void setDelayTimeSec(long sec) {
        this.putProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC, String.valueOf(sec));
    }

    public long getDelayTimeSec() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }

    public void setDelayTimeMs(long timeMs) {
        this.putProperty(MessageConst.PROPERTY_TIMER_DELAY_MS, String.valueOf(timeMs));
    }

    public long getDelayTimeMs() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }

    public void setDeliverTimeMs(long timeMs) {
        this.putProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(timeMs));
    }

    public long getDeliverTimeMs() {
        String t = this.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS);
        if (t != null) {
            return Long.parseLong(t);
        }
        return 0;
    }
}
