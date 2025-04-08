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
package org.apache.rocketmq.broker.client.rebalance;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RebalanceLockManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty(
        "rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));
    private final Lock lock = new ReentrantLock();
    /**
     * 每个消费者组都是独立的
     * 缓存消费者组与队列的锁
     * key: 消费者组
     * value: map        key : MessageQueue
     *                   value：clientId、lastUpdateTimestamp
     */
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable =
        new ConcurrentHashMap<>(1024);

    public boolean isLockAllExpired(final String group) {
        final ConcurrentHashMap<MessageQueue, LockEntry> lockEntryMap = mqLockTable.get(group);
        if (null == lockEntryMap) {
            return true;
        }
        for (LockEntry entry : lockEntryMap.values()) {
            if (!entry.isExpired()) {
                return false;
            }
        }
        return true;
    }

    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        if (!this.isLocked(group, mq, clientId)) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    LockEntry lockEntry = groupValue.get(mq);
                    if (null == lockEntry) {
                        lockEntry = new LockEntry();
                        lockEntry.setClientId(clientId);
                        groupValue.put(mq, lockEntry);
                        log.info(
                            "RebalanceLockManager#tryLock: lock a message queue which has not been locked yet, "
                                + "group={}, clientId={}, mq={}", group, clientId, mq);
                    }

                    if (lockEntry.isLocked(clientId)) {
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        return true;
                    }

                    String oldClientId = lockEntry.getClientId();

                    if (lockEntry.isExpired()) {
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        log.warn(
                            "RebalanceLockManager#tryLock: try to lock a expired message queue, group={}, mq={}, old "
                                + "client id={}, new client id={}", group, mq, oldClientId, clientId);
                        return true;
                    }

                    log.warn(
                        "RebalanceLockManager#tryLock: message queue has been locked by other client, group={}, "
                            + "mq={}, locked client id={}, current client id={}", group, mq, oldClientId, clientId);
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("RebalanceLockManager#tryLock: unexpected error, group={}, mq={}, clientId={}", group, mq,
                    clientId, e);
            }
        } else {

        }

        return true;
    }

    private boolean isLocked(final String group, final MessageQueue mq, final String clientId) {
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
        if (groupValue != null) {
            LockEntry lockEntry = groupValue.get(mq);
            if (lockEntry != null) {
                boolean locked = lockEntry.isLocked(clientId);
                if (locked) {
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                }

                return locked;
            }
        }

        return false;
    }


    /**
     * 顺序消息-broker-锁定队列(2)消息队列锁定信息保存在 RebalanceLockManager 的ConcurrentMap<String, Map<MessageQueue, LockEntry>> 缓存中
     * 在顺序消息模式下
     * 先区分出哪些队列已经被锁定，哪些没有被锁定
     * 未锁定的队列：没有被其他消费者锁定，直接锁定返回
     *             已被锁定的队列：判断是否过期，如果过期，加锁
     *                                         未过期，不锁定，不添加到已锁定集合中
     */
    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs,
        final String clientId) {
        Set<MessageQueue> lockedMqs = new HashSet<>(mqs.size());
        Set<MessageQueue> notLockedMqs = new HashSet<>(mqs.size());

        for (MessageQueue mq : mqs) {
            // 查询消费组下，某队列是否被该消费者锁定
            if (this.isLocked(group, mq, clientId)) {
                lockedMqs.add(mq);
            } else {
                notLockedMqs.add(mq);
            }
        }

        if (!notLockedMqs.isEmpty()) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    for (MessageQueue mq : notLockedMqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null == lockEntry) {
                            lockEntry = new LockEntry();
                            lockEntry.setClientId(clientId);
                            groupValue.put(mq, lockEntry);
                            log.info(
                                "RebalanceLockManager#tryLockBatch: lock a message which has not been locked yet, "
                                    + "group={}, clientId={}, mq={}", group, clientId, mq);
                        }

                        // 已被客户端锁定，更新锁定时间，返回
                        if (lockEntry.isLocked(clientId)) {
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMqs.add(mq);
                            continue;
                        }

                        String oldClientId = lockEntry.getClientId();

                        // 默认最大锁定时间 1 min
                        // 旧锁定队列的客户端过期，设置锁定队列的新客户端、锁定时间
                        if (lockEntry.isExpired()) {
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn(
                                "RebalanceLockManager#tryLockBatch: try to lock a expired message queue, group={}, "
                                    + "mq={}, old client id={}, new client id={}", group, mq, oldClientId, clientId);
                            lockedMqs.add(mq);
                            continue;
                        }

                        log.warn(
                            "RebalanceLockManager#tryLockBatch: message queue has been locked by other client, "
                                + "group={}, mq={}, locked client id={}, current client id={}", group, mq, oldClientId,
                            clientId);
                    }
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("RebalanceLockManager#tryBatch: unexpected error, group={}, mqs={}, clientId={}", group, mqs,
                    clientId, e);
            }
        }

        return lockedMqs;
    }

    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {
                    for (MessageQueue mq : mqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null != lockEntry) {
                            if (lockEntry.getClientId().equals(clientId)) {
                                groupValue.remove(mq);
                                log.info("RebalanceLockManager#unlockBatch: unlock mq, group={}, clientId={}, mqs={}",
                                    group, clientId, mq);
                            } else {
                                log.warn(
                                    "RebalanceLockManager#unlockBatch: mq locked by other client, group={}, locked "
                                        + "clientId={}, current clientId={}, mqs={}", group, lockEntry.getClientId(),
                                    clientId, mq);
                            }
                        } else {
                            log.warn("RebalanceLockManager#unlockBatch: mq not locked, group={}, clientId={}, mq={}",
                                group, clientId, mq);
                        }
                    }
                } else {
                    log.warn("RebalanceLockManager#unlockBatch: group not exist, group={}, clientId={}, mqs={}", group,
                        clientId, mqs);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("RebalanceLockManager#unlockBatch: unexpected error, group={}, mqs={}, clientId={}", group, mqs,
                clientId);
        }
    }

    static class LockEntry {
        private String clientId;
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        public boolean isExpired() {
            boolean expired =
                (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;

            return expired;
        }
    }
}
