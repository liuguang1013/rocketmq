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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public abstract class RebalanceImpl {
    protected static final Logger log = LoggerFactory.getLogger(RebalanceImpl.class);

    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<>(64);
    protected final ConcurrentMap<MessageQueue, PopProcessQueue> popProcessQueueTable = new ConcurrentHashMap<>(64);

    /**
     * updateTopicRouteInfoFromNameServer 获取从 NameSrv 获取 topicRouteData 后，
     * topicRouteData2TopicSubscribeInfo 会将 TopicRouteData 可读的队列数量，封装成 MessageQueue 对象，最终放入缓存
     */
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<>();

    /**
     * 在 消费者启动的时候start()，会将订阅信息复制进来 copySubscription
     * 存放 topic 本身订阅信息，
     * 对于集群消息类型来说，还会构建 ConsumerGroup 的重试消息
     */
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner = new ConcurrentHashMap<>();
    /**
     *
      */
    protected String consumerGroup;
    protected MessageModel messageModel;
    /**
     * 消费者启动时候 start()，默认设置 AllocateMessageQueueAveragely
     */
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mQClientFactory;
    private static final int TIMEOUT_CHECK_TIMES = 3;
    private static final int QUERY_ASSIGNMENT_TIMEOUT = 3000;
    /**
     * todo
     */
    private Map<String, String> topicBrokerRebalance = new ConcurrentHashMap<>();
    /**
     * todo
     */
    private Map<String, String> topicClientRebalance = new ConcurrentHashMap<>();

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    /**
     *  todo： lock 什么？
     */
    public void unlock(final MessageQueue mq, final boolean oneway) {
        // 查找 broker 信息： 地址、主从标识、broker 版本
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(
                this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<>();

        for (Map.Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (pq.isDropped()) {
                continue;
            }

            String destBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(mq);
            Set<MessageQueue> mqs = result.get(destBrokerName);
            if (null == mqs) {
                mqs = new HashSet<>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    public boolean lock(final MessageQueue mq) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("message queue lock {}, {} {}", lockOK ? "OK" : "Failed", this.consumerGroup, mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    public void lockAll() {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (lockOKMQSet.contains(mq)) {
                                if (!processQueue.isLocked()) {
                                    log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                                }
                                processQueue.setLocked(true);
                                processQueue.setLastLockTimestamp(System.currentTimeMillis());
                            } else {
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    public boolean clientRebalance(String topic) {
        return true;
    }

    /**
     * 消费者启动的时候，mQClientFactory#start 方法中会开启 rebalanceService 定时任务
     * 最终进入该方法
     */
    public boolean doRebalance(final boolean isOrder) {
        boolean balanced = true;
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            // 遍历 订阅的 topic 的订阅信息
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    // 对于 push 消费者：判断是否为 广播、顺序消息 并且 尝试队列分配
                    if (!clientRebalance(topic) && tryQueryAssignment(topic)) {
                        // todo： 待看
                        boolean result = this.getRebalanceResultFromBroker(topic, isOrder);
                        if (!result) {
                            balanced = false;
                        }
                    } else {
                        /**
                         * 通过 topic 重平衡
                         * 对于 集群消费类型：
                         *   获取 topic 的 MessageQueue 列表、clientId 列表
                         *   通过 AllocateMessageQueueStrategy 策略为各个clientId消费者分配 MessageQueue
                         *   新增/移除 processQueueTable 缓存：
                         *       移除涉及到：进行 topic 偏移量的持久化
                         *       新增涉及到：查询分配的消费队列的最大偏移量：先从文件中获取，再去broker主节点获取
                         *                构建 pullRequestList 请求，向 PullMessageService 的 阻塞队列添加
                         *   processQueueTable缓存改变：
                         *       更新消费端 SubscriptionData 版本、调整消费端拉取阈值参数、向所有 broker 发送心跳、调用messageQueueListener
                         */
                        boolean result = this.rebalanceByTopic(topic, isOrder);
                        if (!result) {
                            balanced = false;
                        }
                    }
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalance Exception", e);
                        balanced = false;
                    }
                }
            }
        }

        //
        this.truncateMessageQueueNotMyTopic();

        return balanced;
    }

    private boolean tryQueryAssignment(String topic) {
        if (topicClientRebalance.containsKey(topic)) {
            return false;
        }

        if (topicBrokerRebalance.containsKey(topic)) {
            return true;
        }
        String strategyName = allocateMessageQueueStrategy != null ? allocateMessageQueueStrategy.getName() : null;
        int retryTimes = 0;
        while (retryTimes++ < TIMEOUT_CHECK_TIMES) {
            try {
                Set<MessageQueueAssignment> resultSet = mQClientFactory.queryAssignment(topic, consumerGroup,
                    strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT / TIMEOUT_CHECK_TIMES * retryTimes);
                topicBrokerRebalance.put(topic, topic);
                return true;
            } catch (Throwable t) {
                if (!(t instanceof RemotingTimeoutException)) {
                    log.error("tryQueryAssignment error.", t);
                    topicClientRebalance.put(topic, topic);
                    return false;
                }
            }
        }

        if (retryTimes >= TIMEOUT_CHECK_TIMES) {
            // if never success before and timeout exceed TIMEOUT_CHECK_TIMES, force client rebalance
            topicClientRebalance.put(topic, topic);
            return false;
        }
        return true;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    /**
     *
     * 对于 集群消费类型：
     *  获取 topic 的 MessageQueue 列表、clientId 列表
     *  通过 AllocateMessageQueueStrategy 策略为各个clientId消费者分配 MessageQueue
     *  新增/移除 processQueueTable 缓存：
     *      移除涉及到：进行 topic 偏移量的持久化
     *      新增涉及到：查询分配的消费队列的最大偏移量：先从文件中获取，再去broker主节点获取
     *               构建 pullRequestList 请求，向 PullMessageService 的 阻塞队列添加
     *  processQueueTable缓存改变：
     *      更新消费端 SubscriptionData 版本、调整消费端拉取阈值参数、向所有 broker 发送心跳、调用messageQueueListener
     */
    private boolean rebalanceByTopic(final String topic, final boolean isOrder) {
        boolean balanced = true;
        switch (messageModel) {
            // todo：待看 广播类型消息
            case BROADCASTING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}", consumerGroup, topic, mqSet, mqSet);
                    }

                    balanced = mqSet.equals(getWorkingMessageQueue(topic));
                } else {
                    this.messageQueueChanged(topic, Collections.<MessageQueue>emptySet(), Collections.<MessageQueue>emptySet());
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            // 集群类型消息
            case CLUSTERING: {
                // 获取 topic 的对映队列消息：
                // 在多 brokerName ，每个broker 都有从节点的情况下，
                // 同一 brokerName 下的主从节点的读写队列都是一致的
                // 不同 brokerName 下读写队列数可能不一致
                // 此处获取的消息队列是，不同brokerName 下的队列的集合
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                // 向随机挑选 broker的主节点，获取消费者 id 列表
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);

                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        // 消息队列 不存在
                        this.messageQueueChanged(topic, Collections.<MessageQueue>emptySet(), Collections.<MessageQueue>emptySet());
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<>();
                    mqAll.addAll(mqSet);

                    Collections.sort(mqAll);
                    Collections.sort(cidAll);
                    // 默认 AllocateMessageQueueAveragely
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    List<MessageQueue> allocateResult = null;
                    try {
                        // 使用策略类 分配消息队列
                        allocateResult = strategy.allocate(
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mqAll,
                            cidAll);
                    } catch (Throwable e) {
                        log.error("allocate message queue exception. strategy name: {}, ex: {}", strategy.getName(), e);
                        return false;
                    }

                    Set<MessageQueue> allocateResultSet = new HashSet<>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }
                    // 对分配的消息队列进行处理：清除/新增 消息队列缓存；删除、新增 都认为是改变
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                            "client rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);
                        //  更新消费端 SubscriptionData 版本、调整消费端拉取阈值参数、向所有 broker 发送心跳、调用messageQueueListener
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }

                    balanced = allocateResultSet.equals(getWorkingMessageQueue(topic));
                }
                break;
            }
            default:
                break;
        }

        return balanced;
    }

    private boolean getRebalanceResultFromBroker(final String topic, final boolean isOrder) {
        String strategyName = this.allocateMessageQueueStrategy.getName();
        Set<MessageQueueAssignment> messageQueueAssignments;
        try {
            messageQueueAssignments = this.mQClientFactory.queryAssignment(topic, consumerGroup,
                strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT);
        } catch (Exception e) {
            log.error("allocate message queue exception. strategy name: {}, ex: {}", strategyName, e);
            return false;
        }

        // null means invalid result, we should skip the update logic
        if (messageQueueAssignments == null) {
            return false;
        }
        Set<MessageQueue> mqSet = new HashSet<>();
        for (MessageQueueAssignment messageQueueAssignment : messageQueueAssignments) {
            if (messageQueueAssignment.getMessageQueue() != null) {
                mqSet.add(messageQueueAssignment.getMessageQueue());
            }
        }
        Set<MessageQueue> mqAll = null;
        boolean changed = this.updateMessageQueueAssignment(topic, messageQueueAssignments, isOrder);
        if (changed) {
            log.info("broker rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, assignmentSet={}",
                strategyName, consumerGroup, topic, this.mQClientFactory.getClientId(), messageQueueAssignments);
            this.messageQueueChanged(topic, mqAll, mqSet);
        }

        return mqSet.equals(getWorkingMessageQueue(topic));
    }

    private Set<MessageQueue> getWorkingMessageQueue(String topic) {
        Set<MessageQueue> queueSet = new HashSet<>();
        for (Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (mq.getTopic().equals(topic) && !pq.isDropped()) {
                queueSet.add(mq);
            }
        }

        for (Entry<MessageQueue, PopProcessQueue> entry : this.popProcessQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            PopProcessQueue pq = entry.getValue();

            if (mq.getTopic().equals(topic) && !pq.isDropped()) {
                queueSet.add(mq);
            }
        }

        return queueSet;
    }

    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        // push
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }

        for (MessageQueue mq : this.popProcessQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                PopProcessQueue pq = this.popProcessQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary pop mq, {}", consumerGroup, mq);
                }
            }
        }

        Iterator<Map.Entry<String, String>> clientIter = topicClientRebalance.entrySet().iterator();
        while (clientIter.hasNext()) {
            if (!subTable.containsKey(clientIter.next().getKey())) {
                clientIter.remove();
            }
        }

        Iterator<Map.Entry<String, String>> brokerIter = topicBrokerRebalance.entrySet().iterator();
        while (brokerIter.hasNext()) {
            if (!subTable.containsKey(brokerIter.next().getKey())) {
                brokerIter.remove();
            }
        }
    }

    /**
     * 对分配的消息队列进行处理：清除/新增 消息队列缓存
     *
     * 在新增 ProcessQueue 缓存的时候，需要获取消息拉取起始位置，
     * 默认是 CONSUME_FROM_LAST_OFFSET，先从本地文件获取，再在 broker 主节点获取
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        boolean changed = false;

        // drop process queues no longer belong me
        // 筛选出 不属于  clientId 的 MessageQueue 保存到 removeQueueMap
        HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<>(this.processQueueTable.size());
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            // 设置为丢弃状态的 ProcessQueue ，后面在 pullMessage 中不会处理 PullRequest
            if (mq.getTopic().equals(topic)) {
                // processQueueTable 中 MessageQueue  是否 在根据 策略筛选出队列中
                if (!mqSet.contains(mq)) {
                    pq.setDropped(true);
                    removeQueueMap.put(mq, pq);
                }
                // push 类型，并且 超过最大的拉取时间间隔
                else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                    pq.setDropped(true);
                    removeQueueMap.put(mq, pq);
                    log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                        consumerGroup, mq);
                }
            }
        }

        // remove message queues no longer belong me
        // 移除不属于 clientId 的 MessageQueue
        for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();
            // offsetStore：持久化 topic 的消费位置、 清除中offsetTable缓存
            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                this.processQueueTable.remove(mq);
                changed = true;
                log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
            }
        }

        // add new message queue
        // 添加新的message queue 、构建 PullRequest 请求列表
        boolean allMQLocked = true;
        List<PullRequest> pullRequestList = new ArrayList<>();
        for (MessageQueue mq : mqSet) {
            if (!this.processQueueTable.containsKey(mq)) {
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    allMQLocked = false;
                    continue;
                }
                // 清除 offsetStore 中offsetTable缓存
                this.removeDirtyOffset(mq);
                // 创建 ProcessQueue 原始对象
                ProcessQueue pq = createProcessQueue(topic);
                pq.setLocked(true);
                // 获取拉取的位置，获取的是 消费队列 最后一个消息
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }

        }

        if (!allMQLocked) {
            // todo ：
            mQClientFactory.rebalanceLater(500);
        }

        // 发送拉取请求： 向 PullMessageService 的 阻塞队列添加 pullRequestList 请求
        this.dispatchPullRequest(pullRequestList, 500);

        return changed;
    }

    private boolean updateMessageQueueAssignment(final String topic, final Set<MessageQueueAssignment> assignments,
        final boolean isOrder) {
        boolean changed = false;

        Map<MessageQueue, MessageQueueAssignment> mq2PushAssignment = new HashMap<>();
        Map<MessageQueue, MessageQueueAssignment> mq2PopAssignment = new HashMap<>();
        for (MessageQueueAssignment assignment : assignments) {
            MessageQueue messageQueue = assignment.getMessageQueue();
            if (messageQueue == null) {
                continue;
            }
            if (MessageRequestMode.POP == assignment.getMode()) {
                mq2PopAssignment.put(messageQueue, assignment);
            } else {
                mq2PushAssignment.put(messageQueue, assignment);
            }
        }

        if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            if (mq2PopAssignment.isEmpty() && !mq2PushAssignment.isEmpty()) {
                //pop switch to push
                //subscribe pop retry topic
                try {
                    final String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    getSubscriptionInner().put(retryTopic, subscriptionData);
                } catch (Exception ignored) {
                }

            } else if (!mq2PopAssignment.isEmpty() && mq2PushAssignment.isEmpty()) {
                //push switch to pop
                //unsubscribe pop retry topic
                try {
                    final String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
                    getSubscriptionInner().remove(retryTopic);
                } catch (Exception ignored) {
                }

            }
        }

        {
            // drop process queues no longer belong me
            HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<>(this.processQueueTable.size());
            Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, ProcessQueue> next = it.next();
                MessageQueue mq = next.getKey();
                ProcessQueue pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    if (!mq2PushAssignment.containsKey(mq)) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                    } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                        log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                            consumerGroup, mq);
                    }
                }
            }
            // remove message queues no longer belong me
            for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
                MessageQueue mq = entry.getKey();
                ProcessQueue pq = entry.getValue();

                if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                    this.processQueueTable.remove(mq);
                    changed = true;
                    log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }

        {
            HashMap<MessageQueue, PopProcessQueue> removeQueueMap = new HashMap<>(this.popProcessQueueTable.size());
            Iterator<Entry<MessageQueue, PopProcessQueue>> it = this.popProcessQueueTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, PopProcessQueue> next = it.next();
                MessageQueue mq = next.getKey();
                PopProcessQueue pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    if (!mq2PopAssignment.containsKey(mq)) {
                        //the queue is no longer your assignment
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                    } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                        log.error("[BUG]doRebalance, {}, try remove unnecessary pop mq, {}, because pop is pause, so try to fixed it",
                            consumerGroup, mq);
                    }
                }
            }
            // remove message queues no longer belong me
            for (Entry<MessageQueue, PopProcessQueue> entry : removeQueueMap.entrySet()) {
                MessageQueue mq = entry.getKey();
                PopProcessQueue pq = entry.getValue();

                if (this.removeUnnecessaryPopMessageQueue(mq, pq)) {
                    this.popProcessQueueTable.remove(mq);
                    changed = true;
                    log.info("doRebalance, {}, remove unnecessary pop mq, {}", consumerGroup, mq);
                }
            }
        }

        {
            // add new message queue
            boolean allMQLocked = true;
            List<PullRequest> pullRequestList = new ArrayList<>();
            for (MessageQueue mq : mq2PushAssignment.keySet()) {
                if (!this.processQueueTable.containsKey(mq)) {
                    if (isOrder && !this.lock(mq)) {
                        log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                        allMQLocked = false;
                        continue;
                    }

                    this.removeDirtyOffset(mq);
                    ProcessQueue pq = createProcessQueue();
                    pq.setLocked(true);
                    long nextOffset = -1L;
                    try {
                        nextOffset = this.computePullFromWhereWithException(mq);
                    } catch (Exception e) {
                        log.info("doRebalance, {}, compute offset failed, {}", consumerGroup, mq);
                        continue;
                    }

                    if (nextOffset >= 0) {
                        ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                        if (pre != null) {
                            log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                        } else {
                            log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                            PullRequest pullRequest = new PullRequest();
                            pullRequest.setConsumerGroup(consumerGroup);
                            pullRequest.setNextOffset(nextOffset);
                            pullRequest.setMessageQueue(mq);
                            pullRequest.setProcessQueue(pq);
                            pullRequestList.add(pullRequest);
                            changed = true;
                        }
                    } else {
                        log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                    }
                }
            }

            if (!allMQLocked) {
                mQClientFactory.rebalanceLater(500);
            }
            this.dispatchPullRequest(pullRequestList, 500);
        }

        {
            // add new message queue
            List<PopRequest> popRequestList = new ArrayList<>();
            for (MessageQueue mq : mq2PopAssignment.keySet()) {
                if (!this.popProcessQueueTable.containsKey(mq)) {
                    PopProcessQueue pq = createPopProcessQueue();
                    PopProcessQueue pre = this.popProcessQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq pop already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new pop mq, {}", consumerGroup, mq);
                        PopRequest popRequest = new PopRequest();
                        popRequest.setTopic(topic);
                        popRequest.setConsumerGroup(consumerGroup);
                        popRequest.setMessageQueue(mq);
                        popRequest.setPopProcessQueue(pq);
                        popRequest.setInitMode(getConsumeInitMode());
                        popRequestList.add(popRequest);
                        changed = true;
                    }
                }
            }

            this.dispatchPopPullRequest(popRequestList, 500);
        }

        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public boolean removeUnnecessaryPopMessageQueue(final MessageQueue mq, final PopProcessQueue pq) {
        return true;
    }

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    /**
     * When the network is unstable, using this interface may return wrong offset.
     * It is recommended to use computePullFromWhereWithException instead.
     * @param mq
     * @return offset
     */
    @Deprecated
    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract long computePullFromWhereWithException(final MessageQueue mq) throws MQClientException;

    public abstract int getConsumeInitMode();

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList, final long delay);

    public abstract void dispatchPopPullRequest(final List<PopRequest> pullRequestList, final long delay);

    public abstract ProcessQueue createProcessQueue();

    public abstract PopProcessQueue createPopProcessQueue();

    public abstract ProcessQueue createProcessQueue(String topicName);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<MessageQueue, PopProcessQueue> getPopProcessQueueTable() {
        return popProcessQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();

        Iterator<Entry<MessageQueue, PopProcessQueue>> popIt = this.popProcessQueueTable.entrySet().iterator();
        while (popIt.hasNext()) {
            Entry<MessageQueue, PopProcessQueue> next = popIt.next();
            next.getValue().setDropped(true);
        }
        this.popProcessQueueTable.clear();
    }

}
