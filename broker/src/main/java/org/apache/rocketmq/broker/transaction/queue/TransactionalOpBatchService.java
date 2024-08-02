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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class TransactionalOpBatchService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private BrokerController brokerController;
    private TransactionalMessageServiceImpl transactionalMessageService;

    private long wakeupTimestamp = 0;


    public TransactionalOpBatchService(BrokerController brokerController,
                                       TransactionalMessageServiceImpl transactionalMessageService) {
        this.brokerController = brokerController;
        this.transactionalMessageService = transactionalMessageService;
    }

    @Override
    public String getServiceName() {
        return TransactionalOpBatchService.class.getSimpleName();
    }

    @Override
    public void run() {
        // 启动事务op批处理线程!
        LOGGER.info("Start transaction op batch thread!");
        // 3 s
        long checkInterval = brokerController.getBrokerConfig().getTransactionOpBatchInterval();
        wakeupTimestamp = System.currentTimeMillis() + checkInterval;

        // 不停止
        while (!this.isStopped()) {
            // 达到预热时间
            long interval = wakeupTimestamp - System.currentTimeMillis();
            if (interval <= 0) {
                interval = 0;
                // 当达到执行间隔，开始释放栅栏
                // 唤醒线程
                wakeup();
            }
            // 挂起线程，直到等待超时
            this.waitForRunning(interval);
        }
        LOGGER.info("End transaction op batch thread!");
    }

    /**
     * 在唤醒线程
     */
    @Override
    protected void onWaitEnd() {
        wakeupTimestamp = transactionalMessageService.batchSendOpMessage();
    }
}
