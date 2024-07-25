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

        // todo：这部分和消费者注册有关，待看
        try {
            //
            Collection<ConsumerFilterData> filterDatas = consumerFilterManager.get(request.getTopic());

            if (filterDatas == null || filterDatas.isEmpty()) {
                return;
            }

            Iterator<ConsumerFilterData> iterator = filterDatas.iterator();
            BitsArray filterBitMap = BitsArray.create(
                this.consumerFilterManager.getBloomFilter().getM()
            );

            long startTime = System.currentTimeMillis();
            while (iterator.hasNext()) {
                ConsumerFilterData filterData = iterator.next();

                if (filterData.getCompiledExpression() == null) {
                    log.error("[BUG] Consumer in filter manager has no compiled expression! {}", filterData);
                    continue;
                }

                if (filterData.getBloomFilterData() == null) {
                    log.error("[BUG] Consumer in filter manager has no bloom data! {}", filterData);
                    continue;
                }

                Object ret = null;
                try {
                    MessageEvaluationContext context = new MessageEvaluationContext(request.getPropertiesMap());

                    ret = filterData.getCompiledExpression().evaluate(context);
                } catch (Throwable e) {
                    log.error("Calc filter bit map error!commitLogOffset={}, consumer={}, {}", request.getCommitLogOffset(), filterData, e);
                }

                log.debug("Result of Calc bit map:ret={}, data={}, props={}, offset={}", ret, filterData, request.getPropertiesMap(), request.getCommitLogOffset());

                // eval true
                if (ret != null && ret instanceof Boolean && (Boolean) ret) {
                    consumerFilterManager.getBloomFilter().hashTo(
                        filterData.getBloomFilterData(),
                        filterBitMap
                    );
                }
            }

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
