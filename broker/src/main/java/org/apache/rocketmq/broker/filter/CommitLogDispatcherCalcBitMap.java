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

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.store.DispatchRequest;

import java.util.Collection;
import java.util.Iterator;

/**
 * Calculate bit map of filter.
 * 计算消息位图
 * RocketMQ 的消息存储在 CommitLog 中，CommitLog 是 RocketMQ 的核心存储结构，用于存储所有消息的物理日志。
 * 当消息被发送到 Broker 并持久化到 CommitLog 后，RocketMQ 需要一种机制来跟踪哪些消息已经被消费组中的消费者所消费，哪些消息还未被消费。
 *
 * CommitLogDispatcherCalcBitMap 的功能就是基于 CommitLog 的消息位置信息（物理偏移量）来维护一个位图，记录哪些消息已经被消费。
 * 这个位图能够帮助 RocketMQ 快速判断消息是否已被特定的消费组消费，从而避免重复消费或跳过未消费的消息。
 *
 * 位图（BitMap）是一种高效的数据结构，它使用单个比特位来表示一个元素的状态，如已消费或未消费。
 * 在 RocketMQ 的上下文中，每个比特位对应 CommitLog 中的一个消息，如果该消息已被消费，对应的比特位会被设置为 1；否则，比特位保持为 0。
 */
public class CommitLogDispatcherCalcBitMap implements CommitLogDispatcher {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    protected final BrokerConfig brokerConfig;
    protected final ConsumerFilterManager consumerFilterManager;

    public CommitLogDispatcherCalcBitMap(BrokerConfig brokerConfig, ConsumerFilterManager consumerFilterManager) {
        this.brokerConfig = brokerConfig;
        this.consumerFilterManager = consumerFilterManager;
    }

    @Override
    public void dispatch(DispatchRequest request) {
        // 默认不打开
        if (!this.brokerConfig.isEnableCalcFilterBitMap()) {
            return;
        }

        try {
            /**
             * consumerFilterManager 中 topic 的过滤信息缓存，与消费者注册有关，
             * @see org.apache.rocketmq.broker.filter.ConsumerFilterManager#register(String, String, String, String, long)
             */
            Collection<ConsumerFilterData> filterDatas = consumerFilterManager.get(request.getTopic());

            // 对于  消费者组的  SelectorType 是 tag 类型 ，注册的时候直接返回，不会保存 ConsumerFilterData 数据。
            if (filterDatas == null || filterDatas.isEmpty()) {
                return;
            }

            /**
             * 遍历 Topic 各个消费者组的 过滤信息
             */
            Iterator<ConsumerFilterData> iterator = filterDatas.iterator();
            // 创建位数组
            // todo： 为啥每个消息 分发都要创建 新的？
            BitsArray filterBitMap = BitsArray.create(
                this.consumerFilterManager.getBloomFilter().getM()
            );

            long startTime = System.currentTimeMillis();
            while (iterator.hasNext()) {
                ConsumerFilterData filterData = iterator.next();
                // 编译表达式为空，直接跳过。
                // 默认情况下， 消费者组的  SelectorType 是 tag 类型 ，注册的时候直接返回，不会有 Expression 对象
                if (filterData.getCompiledExpression() == null) {
                    log.error("[BUG] Consumer in filter manager has no compiled expression! {}", filterData);
                    continue;
                }

                /**
                 * 不是 tag 类型，并且表达式不为空才初始化 BloomFilter 数据
                 * @see  org.apache.rocketmq.broker.filter.ConsumerFilterManager#register(String, String, String, String, long)
                 */
                if (filterData.getBloomFilterData() == null) {
                    log.error("[BUG] Consumer in filter manager has no bloom data! {}", filterData);
                    continue;
                }

                Object ret = null;
                try {
                    // 将 消息 的 Properties 属性 封装为 MessageEvaluationContext
                    MessageEvaluationContext context = new MessageEvaluationContext(request.getPropertiesMap());
                    // 使用 编译表达式，评估消息
                    ret = filterData.getCompiledExpression().evaluate(context);
                } catch (Throwable e) {
                    log.error("Calc filter bit map error!commitLogOffset={}, consumer={}, {}", request.getCommitLogOffset(), filterData, e);
                }

                log.debug("Result of Calc bit map:ret={}, data={}, props={}, offset={}", ret, filterData, request.getPropertiesMap(), request.getCommitLogOffset());

                // eval true
                // 经过编译表达式评估 满足要求，计算并记录到 位数组中
                if (ret != null && ret instanceof Boolean && (Boolean) ret) {
                    // 将 BitsArray 中对映的 位 上的值，设置为1
                    consumerFilterManager.getBloomFilter().hashTo(
                        // 某topic在某消费者组下的，所属的多个位置信息：这个在消费者组创建的时候就已经计算出了
                        filterData.getBloomFilterData(),
                        // 位数组
                        filterBitMap
                    );
                }
            }
            // 向每个请求中 设置 位数组的值
            request.setBitMap(filterBitMap.bytes());

            long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(startTime);
            // 1ms
            if (elapsedTime >= 1) {
                log.warn("Spend {} ms to calc bit map, consumerNum={}, topic={}", elapsedTime, filterDatas.size(), request.getTopic());
            }
        } catch (Throwable e) {
            log.error("Calc bit map error! topic={}, offset={}, queueId={}, {}", request.getTopic(), request.getCommitLogOffset(), request.getQueueId(), e);
        }
    }
}
