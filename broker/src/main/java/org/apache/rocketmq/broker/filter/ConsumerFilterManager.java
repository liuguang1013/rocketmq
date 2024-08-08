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

package org.apache.rocketmq.broker.filter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.filter.util.BloomFilterData;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * Consumer filter data manager.Just manage the consumers use expression filter.
 * 消费者过滤器数据管理器。只需管理使用表达式过滤器的消费者。
 * ConsumerFilterManager 提供了一种机制来根据消息标签（Tag）或其他自定义属性来筛选消息，从而决定哪些消息应该被消费者消费。
 * 工作原理
 * 1、消息发送:
 * 生产者发送消息时，可以为消息指定一个或多个标签（Tag）。这些标签将在消息体中作为属性携带。
 * 2、消息消费:
 * 消费者订阅消息时，可以通过设置订阅表达式（Subscription Expression）来指定感兴趣的消息标签。
 * 当消息到达 Broker 时，Broker 根据订阅表达式进行初步的过滤，只将符合条件的消息推送给消费者。
 * 3、ConsumerFilterManager 处理:
 * 当消息到达消费者时，ConsumerFilterManager 会进一步根据消费者设置的过滤规则进行过滤。
 * 如果消息满足过滤条件，则会被传递给消费者的业务处理逻辑；否则，消息将被丢弃或跳过。
 */
public class ConsumerFilterManager extends ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    private static final long MS_24_HOUR = 24 * 3600 * 1000;

    /**
     * 过滤信息缓存
     * key ： Topic
     * value： key： consumer group   value：ConsumerFilterData 包含表达式等信息
     */
    private ConcurrentMap<String/*Topic*/, FilterDataMapByTopic> filterDataByTopic = new ConcurrentHashMap<>(256);

    private transient BrokerController brokerController;
    private transient BloomFilter bloomFilter;

    public ConsumerFilterManager() {
        // just for test
        this.bloomFilter = BloomFilter.createByFn(20, 64);
    }

    public ConsumerFilterManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        // 创建布隆过滤器
        this.bloomFilter = BloomFilter.createByFn(
            // 20
            brokerController.getBrokerConfig().getMaxErrorRateOfBloomFilter(),
            // 32
            brokerController.getBrokerConfig().getExpectConsumerNumUseFilter()
        );
        // then set bit map length of store config.
        // 设置位数组的大小，一个是8位
        brokerController.getMessageStoreConfig().setBitMapLengthConsumeQueueExt(
            this.bloomFilter.getM()
        );
    }

    /**
     * Build consumer filter data.Be care, bloom filter data is not included.
     * 构建消费者过滤器数据。注意，布隆过滤器数据不包括在内。
     * @return maybe null
     */
    public static ConsumerFilterData build(final String topic, final String consumerGroup,
                                           final String expression, final String type, final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return null;
        }

        ConsumerFilterData consumerFilterData = new ConsumerFilterData();
        consumerFilterData.setTopic(topic);
        consumerFilterData.setConsumerGroup(consumerGroup);
        consumerFilterData.setBornTime(System.currentTimeMillis());
        consumerFilterData.setDeadTime(0);
        consumerFilterData.setExpression(expression);
        consumerFilterData.setExpressionType(type);
        consumerFilterData.setClientVersion(clientVersion);
        try {
            consumerFilterData.setCompiledExpression(
                FilterFactory.INSTANCE.get(type).compile(expression)
            );
        } catch (Throwable e) {
            log.error("parse error: expr={}, topic={}, group={}, error={}", expression, topic, consumerGroup, e.getMessage());
            return null;
        }

        return consumerFilterData;
    }

    public void register(final String consumerGroup, final Collection<SubscriptionData> subList) {
        for (SubscriptionData subscriptionData : subList) {
            register(
                subscriptionData.getTopic(),
                consumerGroup,
                subscriptionData.getSubString(),
                subscriptionData.getExpressionType(),
                subscriptionData.getSubVersion()
            );
        }

        // make illegal topic dead.
        Collection<ConsumerFilterData> groupFilterData = getByGroup(consumerGroup);

        Iterator<ConsumerFilterData> iterator = groupFilterData.iterator();
        while (iterator.hasNext()) {
            ConsumerFilterData filterData = iterator.next();

            boolean exist = false;
            for (SubscriptionData subscriptionData : subList) {
                if (subscriptionData.getTopic().equals(filterData.getTopic())) {
                    exist = true;
                    break;
                }
            }

            if (!exist && !filterData.isDead()) {
                filterData.setDeadTime(System.currentTimeMillis());
                log.info("Consumer filter changed: {}, make illegal topic dead:{}", consumerGroup, filterData);
            }
        }
    }

    public boolean register(final String topic, final String consumerGroup, final String expression,
        final String type, final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return false;
        }

        if (expression == null || expression.length() == 0) {
            return false;
        }

        FilterDataMapByTopic filterDataMapByTopic = this.filterDataByTopic.get(topic);

        if (filterDataMapByTopic == null) {
            FilterDataMapByTopic temp = new FilterDataMapByTopic(topic);
            FilterDataMapByTopic prev = this.filterDataByTopic.putIfAbsent(topic, temp);
            filterDataMapByTopic = prev != null ? prev : temp;
        }

        BloomFilterData bloomFilterData = bloomFilter.generate(consumerGroup + "#" + topic);

        return filterDataMapByTopic.register(consumerGroup, expression, type, bloomFilterData, clientVersion);
    }

    public void unRegister(final String consumerGroup) {
        for (Entry<String, FilterDataMapByTopic> entry : filterDataByTopic.entrySet()) {
            entry.getValue().unRegister(consumerGroup);
        }
    }

    public ConsumerFilterData get(final String topic, final String consumerGroup) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }

        return this.filterDataByTopic.get(topic).getGroupFilterData().get(consumerGroup);
    }

    public Collection<ConsumerFilterData> getByGroup(final String consumerGroup) {
        Collection<ConsumerFilterData> ret = new HashSet<>();

        Iterator<FilterDataMapByTopic> topicIterator = this.filterDataByTopic.values().iterator();
        while (topicIterator.hasNext()) {
            FilterDataMapByTopic filterDataMapByTopic = topicIterator.next();

            Iterator<ConsumerFilterData> filterDataIterator = filterDataMapByTopic.getGroupFilterData().values().iterator();

            while (filterDataIterator.hasNext()) {
                ConsumerFilterData filterData = filterDataIterator.next();

                if (filterData.getConsumerGroup().equals(consumerGroup)) {
                    ret.add(filterData);
                }
            }
        }

        return ret;
    }

    public final Collection<ConsumerFilterData> get(final String topic) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }

        return this.filterDataByTopic.get(topic).getGroupFilterData().values();
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        if (this.brokerController != null) {
            return BrokerPathConfigHelper.getConsumerFilterPath(
                this.brokerController.getMessageStoreConfig().getStorePathRootDir()
            );
        }
        return BrokerPathConfigHelper.getConsumerFilterPath("./unit_test");
    }

    @Override
    public void decode(final String jsonString) {
        ConsumerFilterManager load = RemotingSerializable.fromJson(jsonString, ConsumerFilterManager.class);
        if (load != null && load.filterDataByTopic != null) {
            boolean bloomChanged = false;
            for (Entry<String, FilterDataMapByTopic> entry : load.filterDataByTopic.entrySet()) {
                FilterDataMapByTopic dataMapByTopic = entry.getValue();
                if (dataMapByTopic == null) {
                    continue;
                }

                for (Entry<String, ConsumerFilterData> groupEntry : dataMapByTopic.getGroupFilterData().entrySet()) {

                    ConsumerFilterData filterData = groupEntry.getValue();

                    if (filterData == null) {
                        continue;
                    }

                    try {
                        filterData.setCompiledExpression(
                                FilterFactory.INSTANCE.get(filterData.getExpressionType()).compile(filterData.getExpression())
                        );
                    } catch (Exception e) {
                        log.error("load filter data error, " + filterData, e);
                    }

                    // check whether bloom filter is changed
                    // if changed, ignore the bit map calculated before.
                    if (!this.bloomFilter.isValid(filterData.getBloomFilterData())) {
                        bloomChanged = true;
                        log.info("Bloom filter is changed!So ignore all filter data persisted! {}, {}", this.bloomFilter, filterData.getBloomFilterData());
                        break;
                    }

                    log.info("load exist consumer filter data: {}", filterData);

                    if (filterData.getDeadTime() == 0) {
                        // we think all consumers are dead when load
                        long deadTime = System.currentTimeMillis() - 30 * 1000;
                        filterData.setDeadTime(
                                deadTime <= filterData.getBornTime() ? filterData.getBornTime() : deadTime
                        );
                    }
                }
            }

            if (!bloomChanged) {
                this.filterDataByTopic = load.filterDataByTopic;
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        // clean
        {
            clean();
        }
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    /**
     * 清除 死亡时间 >= 默认 24 小时 的 数据
     * todo：具体干啥还是没明白
     */
    public void clean() {
        Iterator<Map.Entry<String, FilterDataMapByTopic>> topicIterator = this.filterDataByTopic.entrySet().iterator();
        while (topicIterator.hasNext()) {
            Map.Entry<String/*Topic*/, FilterDataMapByTopic> filterDataMapByTopic = topicIterator.next();

            Iterator<Map.Entry<String/*consumer group*/, ConsumerFilterData>> filterDataIterator
                = filterDataMapByTopic.getValue().getGroupFilterData().entrySet().iterator();

            while (filterDataIterator.hasNext()) {
                Map.Entry<String, ConsumerFilterData> filterDataByGroup = filterDataIterator.next();

                ConsumerFilterData filterData = filterDataByGroup.getValue();
                // 死亡时间 >= 默认 24 小时
                if (filterData.howLongAfterDeath() >= (this.brokerController == null ? MS_24_HOUR : this.brokerController.getBrokerConfig().getFilterDataCleanTimeSpan())) {
                    log.info("Remove filter consumer {}, died too long!", filterDataByGroup.getValue());
                    filterDataIterator.remove();
                }
            }

            if (filterDataMapByTopic.getValue().getGroupFilterData().isEmpty()) {
                log.info("Topic has no consumer, remove it! {}", filterDataMapByTopic.getKey());
                topicIterator.remove();
            }
        }
    }

    public ConcurrentMap<String, FilterDataMapByTopic> getFilterDataByTopic() {
        return filterDataByTopic;
    }

    public void setFilterDataByTopic(final ConcurrentHashMap<String, FilterDataMapByTopic> filterDataByTopic) {
        this.filterDataByTopic = filterDataByTopic;
    }

    public static class FilterDataMapByTopic {

        private ConcurrentMap<String/*consumer group*/, ConsumerFilterData> groupFilterData = new ConcurrentHashMap<>();

        private String topic;

        public FilterDataMapByTopic() {
        }

        public FilterDataMapByTopic(String topic) {
            this.topic = topic;
        }

        public void unRegister(String consumerGroup) {
            if (!this.groupFilterData.containsKey(consumerGroup)) {
                return;
            }

            ConsumerFilterData data = this.groupFilterData.get(consumerGroup);

            if (data == null || data.isDead()) {
                return;
            }

            long now = System.currentTimeMillis();

            log.info("Unregister consumer filter: {}, deadTime: {}", data, now);

            data.setDeadTime(now);
        }

        public boolean register(String consumerGroup, String expression, String type, BloomFilterData bloomFilterData,
            long clientVersion) {
            ConsumerFilterData old = this.groupFilterData.get(consumerGroup);

            if (old == null) {
                ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                if (consumerFilterData == null) {
                    return false;
                }
                consumerFilterData.setBloomFilterData(bloomFilterData);

                old = this.groupFilterData.putIfAbsent(consumerGroup, consumerFilterData);
                if (old == null) {
                    log.info("New consumer filter registered: {}", consumerFilterData);
                    return true;
                } else {
                    if (clientVersion <= old.getClientVersion()) {
                        if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                            log.warn("Ignore consumer({} : {}) filter(concurrent), because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                                consumerGroup, topic,
                                clientVersion, old.getClientVersion(),
                                old.getExpressionType(), old.getExpression(),
                                type, expression);
                        }
                        if (clientVersion == old.getClientVersion() && old.isDead()) {
                            reAlive(old);
                            return true;
                        }

                        return false;
                    } else {
                        this.groupFilterData.put(consumerGroup, consumerFilterData);
                        log.info("New consumer filter registered(concurrent): {}, old: {}", consumerFilterData, old);
                        return true;
                    }
                }
            } else {
                if (clientVersion <= old.getClientVersion()) {
                    if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                        log.info("Ignore consumer({}:{}) filter, because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                            consumerGroup, topic,
                            clientVersion, old.getClientVersion(),
                            old.getExpressionType(), old.getExpression(),
                            type, expression);
                    }
                    if (clientVersion == old.getClientVersion() && old.isDead()) {
                        reAlive(old);
                        return true;
                    }

                    return false;
                }

                boolean change = !old.getExpression().equals(expression) || !old.getExpressionType().equals(type);
                if (old.getBloomFilterData() == null && bloomFilterData != null) {
                    change = true;
                }
                if (old.getBloomFilterData() != null && !old.getBloomFilterData().equals(bloomFilterData)) {
                    change = true;
                }

                // if subscribe data is changed, or consumer is died too long.
                if (change) {
                    ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                    if (consumerFilterData == null) {
                        // new expression compile error, remove old, let client report error.
                        this.groupFilterData.remove(consumerGroup);
                        return false;
                    }
                    consumerFilterData.setBloomFilterData(bloomFilterData);

                    this.groupFilterData.put(consumerGroup, consumerFilterData);

                    log.info("Consumer filter info change, old: {}, new: {}, change: {}",
                        old, consumerFilterData, change);

                    return true;
                } else {
                    old.setClientVersion(clientVersion);
                    if (old.isDead()) {
                        reAlive(old);
                    }
                    return true;
                }
            }
        }

        protected void reAlive(ConsumerFilterData filterData) {
            long oldDeadTime = filterData.getDeadTime();
            filterData.setDeadTime(0);
            log.info("Re alive consumer filter: {}, oldDeadTime: {}", filterData, oldDeadTime);
        }

        public final ConsumerFilterData get(String consumerGroup) {
            return this.groupFilterData.get(consumerGroup);
        }

        public final ConcurrentMap<String, ConsumerFilterData> getGroupFilterData() {
            return this.groupFilterData;
        }

        public void setGroupFilterData(final ConcurrentHashMap<String, ConsumerFilterData> groupFilterData) {
            this.groupFilterData = groupFilterData;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(final String topic) {
            this.topic = topic;
        }
    }
}
