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
package org.apache.rocketmq.broker.slave;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.loadbalance.MessageRequestModeManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMetrics;

/**
 * 从节点 同步
 */
public class SlaveSynchronize {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private volatile String masterAddr = null;

    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        if (!StringUtils.equals(this.masterAddr, masterAddr)) {
            LOGGER.info("Update master address from {} to {}", this.masterAddr, masterAddr);
            this.masterAddr = masterAddr;
        }
    }

    /**
     * 从节点向主节点发送请求，同步信息
     */
    public void syncAll() {
        /**
         * 同步 Topic 配置：
         * 获取主节点
         * TopicConfigManager 中 topicConfigTable、DataVersion；
         * TopicQueueMappingManager 中 topicQueueMappingTable、DataVersion
         * 持久化 topics.json、topicQueueMapping.json 文件
         */
        this.syncTopicConfig();
        /**
         *  同步 消费者偏移量
         *  获取主节点 ConsumerOffsetManager 的 OffsetTable、DataVersion
         *  持久化 config/consumerOffset.json
          */
        this.syncConsumerOffset();
        /**
         *  主要是同步主节点的 ScheduleMessageService 的 offsetTable
         *  持久化 config/delayOffset.json
         */
        this.syncDelayOffset();
        /**
         * 主要是同步主节点的 SubscriptionGroupManager 的 SubscriptionGroupTable、DataVersion
         * 持久化 config/subscriptionGroup.json
         */
        this.syncSubscriptionGroupConfig();
        /**
         * 主要是同步主节点的 MessageRequestModeManager 的 messageRequestModeMap
         * 持久化 config/messageRequestMode.json
         */
        this.syncMessageRequestMode();

        if (brokerController.getMessageStoreConfig().isTimerWheelEnable()) {
            this.syncTimerMetrics();
        }
    }

    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        // 主节点不为空，并与当前节点地址不同
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                // GET_ALL_TOPIC_CONFIG
                // TopicConfigManager 中 topicConfigTable、DataVersion；
                // TopicQueueMappingManager中topicQueueMappingTable、DataVersion
                TopicConfigAndMappingSerializeWrapper topicWrapper = this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);

                // 数据版本不一致
                if (!this.brokerController.getTopicConfigManager().getDataVersion().equals(topicWrapper.getDataVersion())) {

                    // 主节点的信息，同步到当前节点
                    this.brokerController.getTopicConfigManager().getDataVersion().assignNewOne(topicWrapper.getDataVersion());

                    ConcurrentMap<String, TopicConfig> newTopicConfigTable = topicWrapper.getTopicConfigTable();
                    //delete
                    ConcurrentMap<String, TopicConfig> topicConfigTable = this.brokerController.getTopicConfigManager().getTopicConfigTable();
                    for (Iterator<Map.Entry<String, TopicConfig>> it = topicConfigTable.entrySet().iterator(); it.hasNext(); ) {
                        Map.Entry<String, TopicConfig> item = it.next();
                        if (!newTopicConfigTable.containsKey(item.getKey())) {
                            it.remove();
                        }
                    }
                    //update
                    topicConfigTable.putAll(newTopicConfigTable);
                    //  持久化 config/topics.json
                    this.brokerController.getTopicConfigManager().persist();
                }

                if (topicWrapper.getTopicQueueMappingDetailMap() != null
                        && !topicWrapper.getMappingDataVersion().equals(this.brokerController.getTopicQueueMappingManager().getDataVersion())) {
                    this.brokerController.getTopicQueueMappingManager().getDataVersion()
                            .assignNewOne(topicWrapper.getMappingDataVersion());

                    ConcurrentMap<String, TopicConfig> newTopicConfigTable = topicWrapper.getTopicConfigTable();
                    //delete
                    ConcurrentMap<String, TopicConfig> topicConfigTable = this.brokerController.getTopicConfigManager().getTopicConfigTable();
                    for (Iterator<Map.Entry<String, TopicConfig>> it = topicConfigTable.entrySet().iterator(); it.hasNext(); ) {
                        Map.Entry<String, TopicConfig> item = it.next();
                        if (!newTopicConfigTable.containsKey(item.getKey())) {
                            it.remove();
                        }
                    }
                    //update
                    topicConfigTable.putAll(newTopicConfigTable);
                    // 持久化 config/topicQueueMapping.json
                    this.brokerController.getTopicQueueMappingManager().persist();
                }
                LOGGER.info("Update slave topic config from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                // 获取主节点 ConsumerOffsetManager 对象的 json 串：GET_ALL_CONSUMER_OFFSET
                ConsumerOffsetSerializeWrapper offsetWrapper = this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);

                this.brokerController.getConsumerOffsetManager().getOffsetTable().putAll(offsetWrapper.getOffsetTable());
                this.brokerController.getConsumerOffsetManager().getDataVersion().assignNewOne(offsetWrapper.getDataVersion());
                // 持久化 config/consumerOffset.json
                this.brokerController.getConsumerOffsetManager().persist();
                LOGGER.info("Update slave consumer offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                // 获取主节点 ScheduleMessageService 对象的 json 串 ：GET_ALL_DELAY_OFFSET
                String delayOffset = this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);

                if (delayOffset != null) {
                    // config/delayOffset.json
                    String fileName =
                            StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController
                                    .getMessageStoreConfig().getStorePathRootDir());
                    try {
                        // 持久化 config/delayOffset.json
                        MixAll.string2File(delayOffset, fileName);
                        // 主要是同步主节点的 ScheduleMessageService 的 offsetTable
                        this.brokerController.getScheduleMessageService().loadWhenSyncDelayOffset();
                    } catch (IOException e) {
                        LOGGER.error("Persist file Exception, {}", fileName, e);
                    }
                }
                LOGGER.info("Update slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                // 获取主节点 subscriptionGroupManager 对象的 json：GET_ALL_SUBSCRIPTIONGROUP_CONFIG
                SubscriptionGroupWrapper subscriptionWrapper =
                        this.brokerController.getBrokerOuterAPI()
                                .getAllSubscriptionGroupConfig(masterAddrBak);

                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                        .equals(subscriptionWrapper.getDataVersion())) {

                    SubscriptionGroupManager subscriptionGroupManager =
                            this.brokerController.getSubscriptionGroupManager();
                   // 主要是同步主节点的 SubscriptionGroupManager 的 SubscriptionGroupTable、DataVersion
                    subscriptionGroupManager.getDataVersion().assignNewOne(
                            subscriptionWrapper.getDataVersion());
                    subscriptionGroupManager.getSubscriptionGroupTable().clear();
                    subscriptionGroupManager.getSubscriptionGroupTable().putAll(
                            subscriptionWrapper.getSubscriptionGroupTable());
                    // 持久化 config/subscriptionGroup.json
                    subscriptionGroupManager.persist();
                    LOGGER.info("Update slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                LOGGER.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncMessageRequestMode() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                MessageRequestModeSerializeWrapper messageRequestModeSerializeWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllMessageRequestMode(masterAddrBak);

                MessageRequestModeManager messageRequestModeManager =
                        this.brokerController.getQueryAssignmentProcessor().getMessageRequestModeManager();

                messageRequestModeManager.getMessageRequestModeMap().clear();
                messageRequestModeManager.getMessageRequestModeMap().putAll(
                        messageRequestModeSerializeWrapper.getMessageRequestModeMap()
                );
                messageRequestModeManager.persist();
                LOGGER.info("Update slave Message Request Mode from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncMessageRequestMode Exception, {}", masterAddrBak, e);
            }
        }
    }

    public void syncTimerCheckPoint() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                if (null != brokerController.getMessageStore().getTimerMessageStore() &&
                        !brokerController.getTimerMessageStore().isShouldRunningDequeue()) {
                    // 主要是同步主节点的 TimerCheckPoint 的 LastReadTimeMs、MasterTimerQueueOffset、DataVersion
                    TimerCheckpoint checkpoint = this.brokerController.getBrokerOuterAPI().getTimerCheckPoint(masterAddrBak);
                    if (null != this.brokerController.getTimerCheckpoint()) {
                        this.brokerController.getTimerCheckpoint().setLastReadTimeMs(checkpoint.getLastReadTimeMs());
                        this.brokerController.getTimerCheckpoint().setMasterTimerQueueOffset(checkpoint.getMasterTimerQueueOffset());
                        this.brokerController.getTimerCheckpoint().getDataVersion().assignNewOne(checkpoint.getDataVersion());
                    }
                }
            } catch (Exception e) {
                LOGGER.error("syncTimerCheckPoint Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncTimerMetrics() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                if (null != brokerController.getMessageStore().getTimerMessageStore()) {
                    TimerMetrics.TimerMetricsSerializeWrapper metricsSerializeWrapper =
                            this.brokerController.getBrokerOuterAPI().getTimerMetrics(masterAddrBak);
                    if (!brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getDataVersion().equals(metricsSerializeWrapper.getDataVersion())) {
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getDataVersion().assignNewOne(metricsSerializeWrapper.getDataVersion());
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getTimingCount().clear();
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getTimingCount().putAll(metricsSerializeWrapper.getTimingCount());
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().persist();
                    }
                }
            } catch (Exception e) {
                LOGGER.error("SyncTimerMetrics Exception, {}", masterAddrBak, e);
            }
        }
    }
}
