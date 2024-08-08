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

public enum GetMessageStatus {

    FOUND,

    /**
     * 消息 通过 messageFilter 和 消费队列 tag 或者 commit log 匹配 ，匹配失败
     */
    NO_MATCHED_MESSAGE,

    /**
     * 根据消息绝对偏移量、大小，在commit log 中未找到消息
     */
    MESSAGE_WAS_REMOVING,

    /**
     * 在消费队列中未找到对映的文件
     */
    OFFSET_FOUND_NULL,

    /**
     * 请求获取消息开始的位置，在消息队列中最后一个消息之后
     */
    OFFSET_OVERFLOW_BADLY,

    /**
     * 请求获取消息开始的位置，在消息队列中最后一个消息位置
     */
    OFFSET_OVERFLOW_ONE,

    /**
     * 请求获取消息开始的位置，在消息队列中第一个消息之前
     */
    OFFSET_TOO_SMALL,

    /**
     * 通过 topic queueId 在 consumeQueueTable 中未找到逻辑队列信息
     */
    NO_MATCHED_LOGIC_QUEUE,

    /**
     * 消费队列中不存在消息：字节数0
     */
    NO_MESSAGE_IN_QUEUE,

    /**
     * 偏移量重置
     */
    OFFSET_RESET
}
