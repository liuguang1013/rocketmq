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
package org.apache.rocketmq.store;

public enum PutMessageStatus {
    PUT_OK,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
    /**
     * HookUtils.checkBeforePutMessage
     * 服务不可用情况：
     * 1、消息存储服务已经关闭
     * 2、钩子函数中，当前节点为从节点，并未开启冗余存储功能，返回该状态
     */
    SERVICE_NOT_AVAILABLE,
    CREATE_MAPPED_FILE_FAILED,
    /**
     * HookUtils.checkBeforePutMessage
     * 1、topic 长度检查失败
     * 2、消息体为空
     */
    MESSAGE_ILLEGAL,
    PROPERTIES_SIZE_EXCEEDED,
    /**
     * todo：使用背景
     */
    OS_PAGE_CACHE_BUSY,
    UNKNOWN_ERROR,
    IN_SYNC_REPLICAS_NOT_ENOUGH,
    PUT_TO_REMOTE_BROKER_FAIL,
    LMQ_CONSUME_QUEUE_NUM_EXCEEDED,
    /**
     * HookUtils.handleScheduleMessage transformTimerMessage
     * 触发时间轮流控
     */
    WHEEL_TIMER_FLOW_CONTROL,
    /**
     *
     */
    WHEEL_TIMER_MSG_ILLEGAL,
    WHEEL_TIMER_NOT_ENABLE
}
