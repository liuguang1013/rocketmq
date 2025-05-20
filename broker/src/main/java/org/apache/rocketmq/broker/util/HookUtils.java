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
package org.apache.rocketmq.broker.util;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.timer.TimerMessageStore;

public class HookUtils {

    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final AtomicLong PRINT_TIMES = new AtomicLong(0);

    /**
     * On Linux: The maximum length for a file name is 255 bytes.
     * The maximum combined length of both the file name and path name is 4096 bytes.
     * This length matches the PATH_MAX that is supported by the operating system.
     * The Unicode representation of a character can occupy several bytes,
     * so the maximum number of characters that comprises a path and file name can vary.
     * The actual limitation is the number of bytes in the path and file components,
     * which might correspond to an equal number of characters.
     */
    private static final Integer MAX_TOPIC_LENGTH = 255;

    public static PutMessageResult checkBeforePutMessage(BrokerController brokerController, final MessageExt msg) {
        //  消息存储是否关闭
        // DefaultMessageStore中Shutdown属性为true
        if (brokerController.getMessageStore().isShutdown()) {
            LOG.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }
        // 从节点 并且未打开冗余存储功能
        if (!brokerController.getMessageStoreConfig().isDuplicationEnable() && BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            long value = PRINT_TIMES.getAndIncrement();
            if ((value % 50000) == 0) {
                LOG.warn("message store is in slave mode, so putMessage is forbidden ");
            }
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }
        // 不可写
        if (!brokerController.getMessageStore().getRunningFlags().isWriteable()) {
            long value = PRINT_TIMES.getAndIncrement();
            if ((value % 50000) == 0) {
                LOG.warn("message store is not writeable, so putMessage is forbidden " + brokerController.getMessageStore().getRunningFlags().getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            PRINT_TIMES.set(0);
        }

        final byte[] topicData = msg.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
        boolean retryTopic = msg.getTopic() != null && msg.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
        //检查不是重试Topic，Topic长度，不能大于127字节
        if (!retryTopic && topicData.length > Byte.MAX_VALUE) {
            LOG.warn("putMessage message topic[{}] length too long {}, but it is not supported by broker",
                msg.getTopic(), topicData.length);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        //检查任何类型Topic长度，不能大于255字节
        if (topicData.length > MAX_TOPIC_LENGTH) {
            LOG.warn("putMessage message topic[{}] length too long {}, but it is not supported by broker",
                msg.getTopic(), topicData.length);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        //检查消息Body字节数组不能为null
        if (msg.getBody() == null) {
            LOG.warn("putMessage message topic[{}], but message body is null", msg.getTopic());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        // 系统 PageCache 是否繁忙
        // todo：待看
        if (brokerController.getMessageStore().isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGE_CACHE_BUSY, null);
        }
        return null;
    }

    public static PutMessageResult checkInnerBatch(BrokerController brokerController, final MessageExt msg) {
        // 检查消息包含 INNER_NUM 属性，但是不存在值
        if (msg.getProperties().containsKey(MessageConst.PROPERTY_INNER_NUM)
            && !MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            LOG.warn("[BUG]The message had property {} but is not an inner batch", MessageConst.PROPERTY_INNER_NUM);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        // 检查消息系统标识为批量消息，但是TopicConfig配置中消费队列不是批量消息类型
        if (MessageSysFlag.check(msg.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG)) {
            Optional<TopicConfig> topicConfig = Optional.ofNullable(brokerController.getTopicConfigManager().getTopicConfigTable().get(msg.getTopic()));
            if (!QueueTypeUtils.isBatchCq(topicConfig)) {
                LOG.error("[BUG]The message is an inner batch but cq type is not batch cq");
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            }
        }

        return null;
    }

    /**
     * 为消息设置属性：
     * PROPERTY_REAL_TOPIC
     * PROPERTY_REAL_QUEUE_ID
     * topic
     * queueId
     *
     * 根据是否指定定时发送等级，分为：时间轮、定时发送等级两类
     * 分别进行了相关的约束信息校验
     */
    public static PutMessageResult handleScheduleMessage(BrokerController brokerController, final MessageExtBrokerInner msg) {
        // 获取事物消息类型
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        // 不是事物消息、事物消息的提交类型
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // 判断 topic 不是 rmq_sys_wheel_timer ，因为对于时间较长的消息会重新投递到 commitLog 中。
            if (!isRolledTimerMessage(msg)) {
                // 判断消息指定 定时等级 返回 false
                if (checkIfTimerMessage(msg)) {

                    if (!brokerController.getMessageStoreConfig().isTimerWheelEnable()) {
                        //wheel timer is not enabled, reject the message
                        // 时间轮不允许，拒绝消息
                        return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_NOT_ENABLE, null);
                    }
                    // 延时消息-broker-接收(2)任意延时时间：时间轮
                    PutMessageResult transformRes = transformTimerMessage(brokerController, msg);
                    if (null != transformRes) {
                        return transformRes;
                    }
                }
            }
            // Delay Delivery
            if (msg.getDelayTimeLevel() > 0) {
                // 延时消息-broker-接收(2)固定延时时间：延时等级
                transformDelayLevelMessage(brokerController, msg);
            }
        }
        return null;
    }

    private static boolean isRolledTimerMessage(MessageExtBrokerInner msg) {
        // 定时消息
        return TimerMessageStore.TIMER_TOPIC.equals(msg.getTopic());
    }

    /**
     * 判断消息是否是延时消息
     *  true 是延时消息
     *  false 不是延时消息
     * @param msg
     * @return
     */
    public static boolean checkIfTimerMessage(MessageExtBrokerInner msg) {
        // 对于 存在延时等级的 定时消息 ，清除时间轮定时消息相关的属性
        if (msg.getDelayTimeLevel() > 0) {
            // 清除定时发送延迟 ms
            if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS)) {
                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_TIMER_DELIVER_MS);
            }
            if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)) {
                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_TIMER_DELAY_SEC);
            }
            if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS)) {
                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_TIMER_DELAY_MS);
            }
            return false;
            //return this.defaultMessageStore.getMessageStoreConfig().isTimerInterceptDelayLevel();
        }
        //double check
        // topic 是 rmq_sys_wheel_timer  或者 TIMER_OUT_MS 属性不为空
        if (TimerMessageStore.TIMER_TOPIC.equals(msg.getTopic()) || null != msg.getProperty(MessageConst.PROPERTY_TIMER_OUT_MS)) {
            return false;
        }
        // TIMER_DELIVER_MS、TIMER_DELAY_MS、TIMER_DELAY_SEC
        return null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS)
                || null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS)
                || null != msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC);
    }

    private static PutMessageResult transformTimerMessage(BrokerController brokerController,
        MessageExtBrokerInner msg) {
        //do transform
        int delayLevel = msg.getDelayTimeLevel();
        long deliverMs;

        // 获取消息延时时间
        try {
            if (msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
                deliverMs = System.currentTimeMillis() + Long.parseLong(msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_SEC)) * 1000;
            } else if (msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS) != null) {
                // TIMER_DELAY_MS 延时时间需要当前时间 + 属性值计算
                deliverMs = System.currentTimeMillis() + Long.parseLong(msg.getProperty(MessageConst.PROPERTY_TIMER_DELAY_MS));
            } else {
                // TIMER_DELIVER_MS 直接记录小时的 延时时间
                deliverMs = Long.parseLong(msg.getProperty(MessageConst.PROPERTY_TIMER_DELIVER_MS));
            }
        } catch (Exception e) {
            return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
        }
        /**
         *  大于当前时间
         *  此处判断非常重要：对于 dequeuePutQueue 中，重新投递的消息包含：已到期的消息、还未到期的正常消息。
         *  已到期的消息：不进行 topic 转换，正常进行后续流程。
         *  还未到期的消息：继续替换再次进入时间轮
          */
        if (deliverMs > System.currentTimeMillis()) {
            // 定时发送时间大于 3天
            if (delayLevel <= 0 && deliverMs - System.currentTimeMillis() > brokerController.getMessageStoreConfig().getTimerMaxDelaySec() * 1000L) {
                return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
            }
            // 获取 精度 ：1s
            int timerPrecisionMs = brokerController.getMessageStoreConfig().getTimerPrecisionMs();
            // 对时间进行取整
            if (deliverMs % timerPrecisionMs == 0) {
                deliverMs -= timerPrecisionMs;
            } else {
                deliverMs = deliverMs / timerPrecisionMs * timerPrecisionMs;
            }
            // 判断时间轮存储 是否拒绝存储：触发流控
            if (brokerController.getTimerMessageStore().isReject(deliverMs)) {
                return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_FLOW_CONTROL, null);
            }
            /**
             * 延时消息-broker-接收(2-1)时间轮：properties 保存真实 topic、queueId、延时时间；
             *              调整 topic 为 rmq_sys_wheel_timer、queueId 固定为 0
             */
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TIMER_OUT_MS, deliverMs + "");
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
            msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
            msg.setTopic(TimerMessageStore.TIMER_TOPIC);
            msg.setQueueId(0);
        }
        // todo：为什么此处还要判断是够有这个属性？
        else if (null != msg.getProperty(MessageConst.PROPERTY_TIMER_DEL_UNIQKEY)) {
            return new PutMessageResult(PutMessageStatus.WHEEL_TIMER_MSG_ILLEGAL, null);
        }
        return null;
    }

    public static void transformDelayLevelMessage(BrokerController brokerController, MessageExtBrokerInner msg) {
        // 定时时间等级，不能超过最大 18
        if (msg.getDelayTimeLevel() > brokerController.getScheduleMessageService().getMaxDelayLevel()) {
            msg.setDelayTimeLevel(brokerController.getScheduleMessageService().getMaxDelayLevel());
        }

        /**
         * 延时消息-broker-接收(2-2)延迟等级：properties 保存真实 topic、queueId
         *              调整 topic 为 SCHEDULE_TOPIC_XXXX、queueId 为 delayLevel - 1
         */
        // Backup real topic, queueId
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        //SCHEDULE_TOPIC_XXXX
        msg.setTopic(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
        // 队列 id ：delayLevel - 1;
        msg.setQueueId(ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel()));
    }

    /**
     * 遍历消息：给某个 broker 发送消息
     */
    public static boolean sendMessageBack(BrokerController brokerController, List<MessageExt> msgList, String brokerName, String brokerAddr) {
        try {
            Iterator<MessageExt> it = msgList.iterator();
            while (it.hasNext()) {
                MessageExt msg = it.next();
                msg.setWaitStoreMsgOK(false);
                //
                brokerController.getBrokerOuterAPI().sendMessageToSpecificBroker(brokerAddr, brokerName, msg, "InnerSendMessageBackGroup", 3000);
                it.remove();
            }
        } catch (Exception e) {
            LOG.error("send message back to broker {} addr {} failed", brokerName, brokerAddr, e);
            return false;
        }
        return true;
    }
}
