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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionMetrics;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;
    private static final int MAX_RETRY_TIMES_FOR_ESCAPE = 10;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    private static final int OP_MSG_PULL_NUMS = 32;

    private static final int SLEEP_WHILE_NO_OP = 1000;

    /**
     * 缓存已经提交的事务消息，需要删除的半消息在队列偏移量
     */
    private final ConcurrentHashMap<Integer/*queueId*/, MessageQueueOpContext> deleteContext = new ConcurrentHashMap<>();

    private ServiceThread transactionalOpBatchService;

    /**
     * 缓存事务消息队列
     * topic 不同：
     *          key："RMQ_SYS_TRANS_HALF_TOPIC"
     *          value： "RMQ_SYS_TRANS_OP_HALF_TOPIC"
     */
    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    private TransactionMetrics transactionMetrics;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;

        transactionalOpBatchService = new TransactionalOpBatchService(transactionalMessageBridge.getBrokerController(), this);
        /**
         *  transactionalOpBatchService 继承 ServiceThread ，开启后，定时每 3s 执行一次，在任务结束执行
         *  TransactionalMessageServiceImpl.batchSendOpMessage 批量将 deleteContext 中的消息，保存到 commit log 中
         */
        transactionalOpBatchService.start();
        // 创建事务消息指标类
        transactionMetrics = new TransactionMetrics(BrokerPathConfigHelper.getTransactionMetricsPath(transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getStorePathRootDir()));
        // 将 user.home/store/config/transactionMetrics 文件内容 解析成 TransactionMetricsSerializeWrapper 对象
        transactionMetrics.load();
    }

    @Override
    public TransactionMetrics getTransactionMetrics() {
        return transactionMetrics;
    }

    @Override
    public void setTransactionMetrics(TransactionMetrics transactionMetrics) {
        this.transactionMetrics = transactionMetrics;
    }


    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        // 通过 事务性消息桥接 异步保存事务半消息
        return transactionalMessageBridge.asyncPutHalfMessage(messageInner);
    }

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                checkTime++;
            }
        }
        // 设置属性：事务消息检查次数 为 2
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    private boolean needSkip(MessageExt msgExt) {

        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        // 72 小时
        if (valueOfCurrentMinusBorn
            > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
            * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(
                putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(
                putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                "Send check message, the offset={} restored in queueOffset={} "
                    + "commitLogOffset={} "
                    + "newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            log.error(
                "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                    + "msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener) {
        try {

            // 查询 事务半消息的 topic的配置信息，构建消息队列，默认只有1个
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, msgQueues);
            for (MessageQueue messageQueue : msgQueues) {
                long startTime = System.currentTimeMillis();
                // 转换事务 topic: RMQ_SYS_TRANS_HALF_TOPIC -> RMQ_SYS_TRANS_OP_HALF_TOPIC
                MessageQueue opQueue = getOpQueue(messageQueue);
                // 拉取 事务半消息、事务提交回滚处理消息 队列的消费偏移量
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                        halfOffset, opOffset);
                    continue;
                }
                //记录已经处理的事务半消息的消息队列偏移量。
                List<Long> doneOpOffset = new ArrayList<>();
                //缓存事务半消息在两个默认topic的队列中的偏移量。
                // key：保存事务半消息topic的，value保存提交或回滚事务消息topic的；
                HashMap<Long, Long> removeMap = new HashMap<>();
                // 缓存 RMQ_SYS_TRANS_OP_HALF_TOPIC 消费队列中某位置，
                // 对映的多个提交或回滚的事务半消息在RMQ_SYS_TRANS_HALF_TOPIC中的消费队列中的偏移量集合；
                HashMap<Long, HashSet<Long>> opMsgMap = new HashMap<Long, HashSet<Long>>();
                // 从 RMQ_SYS_TRANS_OP_HALF_TOPIC  的消费队列中，获取32 个消息，并且填充需要移除的map
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, opMsgMap, doneOpOffset);
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                        messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                int getMessageNullCount = 1;
                long newOffset = halfOffset;
                long i = halfOffset;
                long nextOpOffset = pullResult.getNextBeginOffset();
                int putInQueueCount = 0;
                int escapeFailCnt = 0;

                while (true) {
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    // 判断 事务半消息 是否已经提交或者回滚
                    if (removeMap.containsKey(i)) {
                        log.debug("Half offset {} has been committed/rolled back", i);
                        Long removedOpOffset = removeMap.remove(i);
                        opMsgMap.get(removedOpOffset).remove(i);
                        if (opMsgMap.get(removedOpOffset).size() == 0) {
                            opMsgMap.remove(removedOpOffset);
                            doneOpOffset.add(removedOpOffset);
                        }
                    }
                    else {
                        // 从 RMQ_SYS_TRANS_HALF_TOPIC 消息队列中 起始位置开始 拉取消息
                        GetResult getResult = getHalfMsg(messageQueue, i);

                        MessageExt msgExt = getResult.getMsg();
                        if (msgExt == null) {
                            // 重试一次
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                    messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                    i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }

                        // 从节点 不处理
                        if (this.transactionalMessageBridge.getBrokerController().getBrokerConfig().isEnableSlaveActingMaster()
                            && this.transactionalMessageBridge.getBrokerController().getMinBrokerIdInGroup() == this.transactionalMessageBridge.getBrokerController().getBrokerIdentity().getBrokerId()
                            && BrokerRole.SLAVE.equals(this.transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getBrokerRole())
                        ) {
                            // 重新构建 RMQ_SYS_TRANS_HALF_TOPIC 消息
                            final MessageExtBrokerInner msgInner = this.transactionalMessageBridge.renewHalfMessageInner(msgExt);
                            // 向主节点保存信息
                            final boolean isSuccess = this.transactionalMessageBridge.escapeMessage(msgInner);

                            if (isSuccess) {
                                escapeFailCnt = 0;
                                newOffset = i + 1;
                                i++;
                            } else {
                                log.warn("Escaping transactional message failed {} times! msgId(offsetId)={}, UNIQ_KEY(transactionId)={}",
                                    escapeFailCnt + 1,
                                    msgExt.getMsgId(),
                                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                                if (escapeFailCnt < MAX_RETRY_TIMES_FOR_ESCAPE) {
                                    escapeFailCnt++;
                                    Thread.sleep(100L * (2 ^ escapeFailCnt));
                                } else {
                                    escapeFailCnt = 0;
                                    newOffset = i + 1;
                                    i++;
                                }
                            }
                            continue;
                        }

                        // 消息次数小于 15 或者 大于 72 小时
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            listener.resolveDiscardMsg(msgExt);
                            newOffset = i + 1;
                            i++;
                            continue;
                        }

                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                new Date(msgExt.getStoreTimestamp()));
                            break;
                        }

                        // 当前时间- 消息生成时间，是否在 6分钟的免疫时间内
                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        long checkImmunityTime = transactionTimeout;
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            // 检查是否在免疫时间内。
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt, checkImmunityTimeStr)) {
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            if (0 <= valueOfCurrentMinusBorn && valueOfCurrentMinusBorn < checkImmunityTime) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                    checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                // 在免疫时间内，结束循环
                                break;
                            }
                        }
                        List<MessageExt> opMsg = pullResult == null ? null : pullResult.getMsgFoundList();

                        boolean isNeedCheck = opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime
                            || opMsg != null && opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout
                            || valueOfCurrentMinusBorn <= -1;

                        if (isNeedCheck) {

                            // 将 事务半消息 重新放回到 RMQ_SYS_TRANS_HALF_TOPIC 下，持久化到 commitLog中
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            putInQueueCount++;
                            log.info("Check transaction. real_topic={},uniqKey={},offset={},commitLogOffset={}",
                                    msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC),
                                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                                    msgExt.getQueueOffset(), msgExt.getCommitLogOffset());
                            listener.resolveHalfMsg(msgExt);
                        }
                        else {
                            nextOpOffset = pullResult != null ? pullResult.getNextBeginOffset() : nextOpOffset;
                            // 再次获取 补充 已提交/回滚事务半消息
                            pullResult = fillOpRemoveMap(removeMap, opQueue, nextOpOffset,
                                    halfOffset, opMsgMap, doneOpOffset);
                            if (pullResult == null || pullResult.getPullStatus() == PullStatus.NO_NEW_MSG
                                    || pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
                                    || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {

                                try {
                                    Thread.sleep(SLEEP_WHILE_NO_OP);
                                } catch (Throwable ignored) {
                                }

                            } else {
                                log.info("The miss message offset:{}, pullOffsetOfOp:{}, miniOffset:{} get more opMsg.", i, nextOpOffset, halfOffset);
                            }

                            continue;
                        }
                    }

                    // 增加 RMQ_SYS_TRANS_HALF_TOPIC 消费队列中 已经处理的消息位置
                    newOffset = i + 1;
                    i++;
                }
                // 更新  RMQ_SYS_TRANS_HALF_TOPIC 消费队列的偏移量
                if (newOffset != halfOffset) {
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                // 只加1
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
                GetResult getResult = getHalfMsg(messageQueue, newOffset);
                pullResult = pullOpMsg(opQueue, newOpOffset, 1);
                long maxMsgOffset = getResult.getPullResult() == null ? newOffset : getResult.getPullResult().getMaxOffset();
                long maxOpOffset = pullResult == null ? newOpOffset : pullResult.getMaxOffset();
                long msgTime = getResult.getMsg() == null ? System.currentTimeMillis() : getResult.getMsg().getStoreTimestamp();

                log.info("After check, {} opOffset={} opOffsetDiff={} msgOffset={} msgOffsetDiff={} msgTime={} msgTimeDelayInMs={} putInQueueCount={}",
                        messageQueue, newOpOffset, maxOpOffset - newOpOffset, newOffset, maxMsgOffset - newOffset, new Date(msgTime),
                        System.currentTimeMillis() - msgTime, putInQueueCount);
            }
        } catch (Throwable e) {
            log.error("Check error", e);
        }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     *
     * @param removeMap Half message to be remove, key:halfOffset, value: opOffset.
     * @param opQueue Op message queue.
     * @param pullOffsetOfOp The begin offset of op message queue.
     * @param miniOffset The current minimum offset of half message queue. 半消息队列的当前最小偏移量。
     * @param opMsgMap Half message offset in op message
     * @param doneOpOffset Stored op messages that have been processed.
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap, MessageQueue opQueue,
                                       long pullOffsetOfOp, long miniOffset, Map<Long, HashSet<Long>> opMsgMap, List<Long> doneOpOffset) {
        // 从 "RMQ_SYS_TRANS_OP_HALF_TOPIC" 消息队列中，获取多个消息，默认32个
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, OP_MSG_PULL_NUMS);

        if (null == pullResult) {
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
            || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            // 不合法，更新的消费偏移量
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            return pullResult;
        }
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        // 遍历消息列表
        for (MessageExt opMessageExt : opMsg) {
            if (opMessageExt.getBody() == null) {
                log.error("op message body is null. queueId={}, offset={}", opMessageExt.getQueueId(),
                        opMessageExt.getQueueOffset());
                doneOpOffset.add(opMessageExt.getQueueOffset());
                continue;
            }
            HashSet<Long> set = new HashSet<Long>();
            String queueOffsetBody = new String(opMessageExt.getBody(), TransactionalMessageUtil.CHARSET);

            log.debug("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                    opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffsetBody);
            /**
             * TransactionalMessageUtil.REMOVE_TAG 这个删除标识，是在创建 消息的时候就添加的 tag
             * @see #getOpMessage(int, String)
             */
            if (TransactionalMessageUtil.REMOVE_TAG.equals(opMessageExt.getTags())) {
                String[] offsetArray = queueOffsetBody.split(TransactionalMessageUtil.OFFSET_SEPARATOR);
                for (String offset : offsetArray) {
                    Long offsetValue = getLong(offset);
                    if (offsetValue < miniOffset) {
                        continue;
                    }
                    // 事务半消息，key ：在RMQ_SYS_TRANS_HALF_TOPIC 消息队列中偏移量、
                    //           value：在RMQ_SYS_TRANS_OP_HALF_TOPIC 消息已提交/回滚消息队列中偏移量
                    removeMap.put(offsetValue, opMessageExt.getQueueOffset());
                    set.add(offsetValue);
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }

            if (set.size() > 0) {
                opMsgMap.put(opMessageExt.getQueueOffset(), set);
            } else {
                doneOpOffset.add(opMessageExt.getQueueOffset());
            }
        }

        log.debug("Remove map: {}", removeMap);
        log.debug("opMsg map: {}", opMsgMap);

        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
        MessageExt msgExt, String checkImmunityTimeStr) {
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    log.info("removeMap contain prepareQueueOffset. real_topic={},uniqKey={},immunityTime={},offset={}",
                            msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC),
                            msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                            checkImmunityTimeStr,
                            msgExt.getQueueOffset());
                    return true;
                } else {
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq Target Message Queue
     * @param offset Offset in the message queue
     * @param nums Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.parseLong(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.parseInt(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();
        // 拉取消息
        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        if (result != null) {
            getResult.setPullResult(result);
            List<MessageExt> messageExts = result.getMsgFoundList();
            if (messageExts == null || messageExts.size() == 0) {
                return getResult;
            }
            getResult.setMsg(messageExts.get(0));
        }
        return getResult;
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        //事务消息-broker-结束事务-提交-(2)根据commitLogOffset查找事务半消息
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    /**

     * 实际并不是真正删除，而是
     */
    @Override
    public boolean deletePrepareMessage(MessageExt messageExt) {
        // RMQ_SYS_TRANS_HALF_TOPIC 事务半消息的topic，队列只有一个，默认只有 0
        Integer queueId = messageExt.getQueueId();
        MessageQueueOpContext mqContext = deleteContext.get(queueId);
        if (mqContext == null) {
            mqContext = new MessageQueueOpContext(System.currentTimeMillis(), 20000);
            MessageQueueOpContext old = deleteContext.putIfAbsent(queueId, mqContext);
            if (old != null) {
                mqContext = old;
            }
        }
        // 事务消息-broker-结束事务-提交-(4)暂存当前消息的队列偏移量，到MessageQueueOpContext的阻塞队列中
        // todo： 为什么偏移量 还要加 "," 分割？方便后续构建消息体，拼接字符串
        String data = messageExt.getQueueOffset() + TransactionalMessageUtil.OFFSET_SEPARATOR;
        try {
            // 向MessageQueueOpContext中队列的添加
            boolean res = mqContext.getContextQueue().offer(data, 100, TimeUnit.MILLISECONDS);
            if (res) {
                int totalSize = mqContext.getTotalSize().addAndGet(data.length());
                // 阻塞队列中，消息数量大于  默认的 4096，唤醒服务
                if (totalSize > transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize()) {
                    this.transactionalOpBatchService.wakeup();
                }
                // 直接返回
                return true;
            } else {
                // 唤醒 事务消息回查检查服务
                this.transactionalOpBatchService.wakeup();
            }
        } catch (InterruptedException ignore) {
        }

        // 事务消息-broker-结束事务-提交-(4.1)保存消息的队列偏移量，到MessageQueueOpContext的阻塞队列中失败，触发 RMQ_SYS_TRANS_OP_HALF_TOPIC topic消息保存流程

        // 构建 OpMessage，实际是封装：deleteContext中缓存的多个提交消息的偏移量，作为消息体
        Message msg = getOpMessage(queueId, data);

       // 将消息写入到 Commit log 中，topic 是 RMQ_SYS_TRANS_OP_HALF_TOPIC
        if (this.transactionalMessageBridge.writeOp(queueId, msg)) {
            log.warn("Force add remove op data. queueId={}", queueId);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", messageExt.getMsgId(), messageExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {
        if (this.transactionalOpBatchService != null) {
            this.transactionalOpBatchService.shutdown();
        }
        this.getTransactionMetrics().persist();
    }

    /**
     * 事务消息-broker-结束事务-提交-(4.2)构建OpMessage，实际是封装：deleteContext中缓存的多个提交消息的偏移量，作为消息体
     *
     * 构建消息：
     *  topic ：RMQ_SYS_TRANS_OP_HALF_TOPIC
     *  消息体的内容，在deleteContext 的 queueId 对映的阻塞队列中
     *
     *  构建消息体过程中需要的数量，取决于消息体最大值，不大于消息体最大值
     *  < 4096
     */
    public Message getOpMessage(int queueId, String moreData) {
        // RMQ_SYS_TRANS_OP_HALF_TOPIC
        String opTopic = TransactionalMessageUtil.buildOpTopic();
        MessageQueueOpContext mqContext = deleteContext.get(queueId);

        int moreDataLength = moreData != null ? moreData.length() : 0;
        int length = moreDataLength;
        // 4096
        int maxSize = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize();
        if (length < maxSize) {
            int sz = mqContext.getTotalSize().get();
            if (sz > maxSize || length + sz > maxSize) {
                length = maxSize + 100;
            } else {
                length += sz;
            }
        }

        // 保存多个消息的偏移量，通过 "，"
        // length = moreDataLength + mqContext.getTotalSize() 或者 length = maxSize(4096) + 100
        StringBuilder sb = new StringBuilder(length);

        if (moreData != null) {
            sb.append(moreData);
        }

        while (!mqContext.getContextQueue().isEmpty()) {
            if (sb.length() >= maxSize) {
                break;
            }
            // 在阻塞队列中拉取数据
            String data = mqContext.getContextQueue().poll();
            if (data != null) {
                sb.append(data);
            }
        }

        if (sb.length() == 0) {
            return null;
        }

        int l = sb.length() - moreDataLength;
        // 重置数据大小。
        mqContext.getTotalSize().addAndGet(-l);
        // 设置上次写入时间
        mqContext.setLastWriteTimestamp(System.currentTimeMillis());

        return new Message(opTopic, TransactionalMessageUtil.REMOVE_TAG,
                sb.toString().getBytes(TransactionalMessageUtil.CHARSET));
    }

    /**
     * 将缓存在 deleteContext 中的 "RMQ_SYS_TRANS_HALF_TOPIC" topic的队列id 对映的 已消费或者已回滚的消息的偏移量
     * 构建消息，并保存到 commitLog 中
     */
    public long batchSendOpMessage() {
        long startTime = System.currentTimeMillis();
        try {
            long firstTimestamp = startTime;
            // 要发送消息的 map
            Map<Integer/*queueId*/, Message> sendMap = null;
            // 默认 3s
            long interval = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpBatchInterval();
            // 4096
            int maxSize = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize();
            // 标识：MessageQueueOpContext 中的数据量是否大于 4096
            boolean overSize = false;
            /**
             *  遍历 deleteContext 集合，
             */
            for (Map.Entry<Integer/*queueId*/, MessageQueueOpContext> entry : deleteContext.entrySet()) {
                MessageQueueOpContext mqContext = entry.getValue();
                //no msg in contextQueue
                if (mqContext.getTotalSize().get() <= 0 || mqContext.getContextQueue().size() == 0 ||
                        // wait for the interval
                        mqContext.getTotalSize().get() < maxSize &&
                                startTime - mqContext.getLastWriteTimestamp() < interval) {
                    continue;
                }

                if (sendMap == null) {
                    sendMap = new HashMap<>();
                }
                // 构建消息：在 deleteContext 队列对映的MessageQueueOpContext 中，获取消息，
                // 获取的个数，以消息体不大于最大消息体为准
                Message opMsg = getOpMessage(entry.getKey(), null);
                if (opMsg == null) {
                    continue;
                }
                sendMap.put(entry.getKey(), opMsg);
                firstTimestamp = Math.min(firstTimestamp, mqContext.getLastWriteTimestamp());
                // 在构建消息消耗到队列中的数据后，数据量依旧 > 最大数量（4096）
                if (mqContext.getTotalSize().get() >= maxSize) {
                    overSize = true;
                }
            }

            if (sendMap != null) {
                for (Map.Entry<Integer, Message> entry : sendMap.entrySet()) {
                    // 向 commit log 写入 消息
                    if (!this.transactionalMessageBridge.writeOp(entry.getKey(), entry.getValue())) {
                        log.error("Transaction batch op message write failed. body is {}, queueId is {}",
                                new String(entry.getValue().getBody(), TransactionalMessageUtil.CHARSET), entry.getKey());
                    }
                }
            }

            log.debug("Send op message queueIds={}", sendMap == null ? null : sendMap.keySet());

            //wait for next batch remove
            long wakeupTimestamp = firstTimestamp + interval;
            // 队列中数据没有超过最大值，并且唤醒时间 大于开始时间
            if (!overSize && wakeupTimestamp > startTime) {
                return wakeupTimestamp;
            }
        } catch (Throwable t) {
            log.error("batchSendOp error.", t);
        }

        return 0L;
    }

    public Map<Integer, MessageQueueOpContext> getDeleteContext() {
        return this.deleteContext;
    }
}
