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
package org.apache.rocketmq.remoting.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;

public class ClientMetadata {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String/* Topic */, ConcurrentMap<MessageQueue, String/*brokerName*/>> topicEndPointsTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
        new ConcurrentHashMap<>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable =
        new ConcurrentHashMap<>();

    public void freshTopicRoute(String topic, TopicRouteData topicRouteData) {
        if (topic == null
            || topicRouteData == null) {
            return;
        }
        TopicRouteData old = this.topicRouteTable.get(topic);
        if (!topicRouteData.topicRouteDataChanged(old)) {
            return ;
        }
        {
            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
            }
        }
        {
            ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, topicRouteData);
            if (mqEndPoints != null
                    && !mqEndPoints.isEmpty()) {
                topicEndPointsTable.put(topic, mqEndPoints);
            }
        }
    }

    public String getBrokerNameFromMessageQueue(final MessageQueue mq) {
        if (topicEndPointsTable.get(mq.getTopic()) != null
                && !topicEndPointsTable.get(mq.getTopic()).isEmpty()) {
            return topicEndPointsTable.get(mq.getTopic()).get(mq);
        }
        return mq.getBrokerName();
    }

    public void refreshClusterInfo(ClusterInfo clusterInfo) {
        if (clusterInfo == null
            || clusterInfo.getBrokerAddrTable() == null) {
            return;
        }
        for (Map.Entry<String, BrokerData> entry : clusterInfo.getBrokerAddrTable().entrySet()) {
            brokerAddrTable.put(entry.getKey(), entry.getValue().getBrokerAddrs());
        }
    }

    public String findMasterBrokerAddr(String brokerName) {
        if (!brokerAddrTable.containsKey(brokerName)) {
            return null;
        }
        return brokerAddrTable.get(brokerName).get(MixAll.MASTER_ID);
    }

    public ConcurrentMap<String, HashMap<Long, String>> getBrokerAddrTable() {
        return brokerAddrTable;
    }

    /**
     * 对 TopicRouteData.TopicQueueMappingInfo 数据进行处理，
     * 最后封装成 MessageQueue 和 scope 的映射
     */
    public static ConcurrentMap<MessageQueue, String> topicRouteData2EndpointsForStaticTopic(final String topic, final TopicRouteData route) {
        // 默认不存在
        if (route.getTopicQueueMappingByBroker() == null
                || route.getTopicQueueMappingByBroker().isEmpty()) {
            return new ConcurrentHashMap<>();
        }

        /**
         * 消息队列
         * key：MessageQueue：topic、brokerName、queueId
         * value ： brokerName ，这是通过工具类生成的，TopicQueueMappingUtils.getMockBrokerName(info.getScope())
         *          可能多个 MessageQueue 对映多个 brokerName
         */
        ConcurrentMap<MessageQueue, String/*brokerName*/> mqEndPointsOfBroker = new ConcurrentHashMap<>();


        // 根据范围再次分组
        // key： TopicQueueMappingInfo.scope
        // value：key：brokerName  value：TopicQueueMappingInfo
        Map<String, Map<String, TopicQueueMappingInfo>> mappingInfosByScope = new HashMap<>();

        for (Map.Entry<String, TopicQueueMappingInfo> entry : route.getTopicQueueMappingByBroker().entrySet()) {
            TopicQueueMappingInfo info = entry.getValue();
            String scope = info.getScope();
            if (scope != null) {
                if (!mappingInfosByScope.containsKey(scope)) {
                    mappingInfosByScope.put(scope, new HashMap<>());
                }
                mappingInfosByScope.get(scope).put(entry.getKey(), entry.getValue());
            }
        }

        // 根据 TopicQueueMappingInfo.scope 分组后，
        // 再次遍历对每个 scope 下的 TopicQueueMappingInfo ，封装成 MessageQueue 对象，MessageQueue 中，brokerName 是根据 Scope 生成的 是一样的，topic 也是一样的， 只有 globalId 不同
        // 期间找出 maxTotalNums 值，进行 mqEndPoints 中数据补充
        for (Map.Entry<String, Map<String, TopicQueueMappingInfo>> mapEntry : mappingInfosByScope.entrySet()) {
            String scope = mapEntry.getKey();
            Map<String/*brokerName*/, TopicQueueMappingInfo> topicQueueMappingInfoMap =  mapEntry.getValue();

            ConcurrentMap<MessageQueue, TopicQueueMappingInfo> mqEndPoints = new ConcurrentHashMap<>();
            // 对某scope 下，映射信息排序：根据数据新旧标识,由大到小
            List<Map.Entry<String/*brokerName*/, TopicQueueMappingInfo>> mappingInfos = new ArrayList<>(topicQueueMappingInfoMap.entrySet());
            mappingInfos.sort((o1, o2) -> (int) (o2.getValue().getEpoch() - o1.getValue().getEpoch()));
            // 查找 某topic下，某scope下，所有brokerName中，最大队列数
            int maxTotalNums = 0;

            long maxTotalNumOfEpoch = -1;
            for (Map.Entry<String, TopicQueueMappingInfo> entry : mappingInfos) {
                TopicQueueMappingInfo info = entry.getValue();
                if (info.getEpoch() >= maxTotalNumOfEpoch && info.getTotalQueues() > maxTotalNums) {
                    maxTotalNums = info.getTotalQueues();
                }
                // key：logicId 逻辑id  value：physicalId 物理id
                for (Map.Entry<Integer, Integer> idEntry : entry.getValue().getCurrIdMap().entrySet()) {
                    int globalId = idEntry.getKey();
                    // 创建消息队列
                    MessageQueue mq = new MessageQueue(topic, TopicQueueMappingUtils.getMockBrokerName(info.getScope()), globalId);
                    TopicQueueMappingInfo oldInfo = mqEndPoints.get(mq);
                    if (oldInfo == null ||  oldInfo.getEpoch() <= info.getEpoch()) {
                        mqEndPoints.put(mq, info);
                    }
                }
            }


            //accomplish the static logic queues
            for (int i = 0; i < maxTotalNums; i++) {
                MessageQueue mq = new MessageQueue(topic, TopicQueueMappingUtils.getMockBrokerName(scope), i);
                if (!mqEndPoints.containsKey(mq)) {
                    mqEndPointsOfBroker.put(mq, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST);
                } else {
                    mqEndPointsOfBroker.put(mq, mqEndPoints.get(mq).getBname());
                }
            }
        }
        return mqEndPointsOfBroker;
    }

}
