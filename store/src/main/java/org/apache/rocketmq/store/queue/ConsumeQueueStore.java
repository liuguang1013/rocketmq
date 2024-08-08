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
package org.apache.rocketmq.store.queue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import static java.lang.String.format;
import static org.apache.rocketmq.store.config.StorePathConfigHelper.getStorePathBatchConsumeQueue;
import static org.apache.rocketmq.store.config.StorePathConfigHelper.getStorePathConsumeQueue;

public class ConsumeQueueStore extends AbstractConsumeQueueStore {

    public ConsumeQueueStore(DefaultMessageStore messageStore) {
        super(messageStore);
    }

    @Override
    public void start() {
        log.info("Default ConsumeQueueStore start!");
    }

    @Override
    public boolean load() {
        // 加载 已经存在的 简单消费对列
        boolean cqLoadResult = loadConsumeQueues(getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), CQType.SimpleCQ);
        // 加载 已经存在的 批量消费队列
        boolean bcqLoadResult = loadConsumeQueues(getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), CQType.BatchCQ);
        return cqLoadResult && bcqLoadResult;
    }

    @Override
    public boolean loadAfterDestroy() {
        return true;
    }

    @Override
    public void recover() {
        // 遍历 topic 的队列：消费队列类型： 普通消息消费队列、批量消息消费队列
        // 稀疏队列在加载的时候就恢复过
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                // 恢复：实际就是
                // 设置最大偏移量：在commitlog 的偏移量、
                // 设置mappedFileQueue属性：在多个文件中绝对偏移量
                this.recover(logic);
            }
        }
    }

    @Override
    public boolean recoverConcurrently() {
        int count = 0;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            count += maps.values().size();
        }
        final CountDownLatch countDownLatch = new CountDownLatch(count);
        BlockingQueue<Runnable> recoverQueue = new LinkedBlockingQueue<>();
        final ExecutorService executor = buildExecutorService(recoverQueue, "RecoverConsumeQueueThread_");
        List<FutureTask<Boolean>> result = new ArrayList<>(count);
        try {
            for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
                for (final ConsumeQueueInterface logic : maps.values()) {
                    FutureTask<Boolean> futureTask = new FutureTask<>(() -> {
                        boolean ret = true;
                        try {
                            logic.recover();
                        } catch (Throwable e) {
                            ret = false;
                            log.error("Exception occurs while recover consume queue concurrently, " +
                                "topic={}, queueId={}", logic.getTopic(), logic.getQueueId(), e);
                        } finally {
                            countDownLatch.countDown();
                        }
                        return ret;
                    });

                    result.add(futureTask);
                    executor.submit(futureTask);
                }
            }
            countDownLatch.await();
            for (FutureTask<Boolean> task : result) {
                if (task != null && task.isDone()) {
                    if (!task.get()) {
                        return false;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception occurs while recover consume queue concurrently", e);
            return false;
        } finally {
            executor.shutdown();
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        return true;
    }

    @Override
    public long rollNextFile(ConsumeQueueInterface consumeQueue, final long offset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.rollNextFile(offset);
    }

    public void correctMinOffset(ConsumeQueueInterface consumeQueue, long minCommitLogOffset) {
        // 修正最小的偏移量
        consumeQueue.correctMinOffset(minCommitLogOffset);
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest dispatchRequest) {
        // 先 consumeQueueTable 缓存中获取 某 topic 的多个消费队列信息
        // 再获取某个具体消费队列的信息
        // 消费队列信息中，存在逻辑队列属性，包含多个具体的消息队列文件
        ConsumeQueueInterface cq = this.findOrCreateConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());

        this.putMessagePositionInfoWrapper(cq, dispatchRequest);
    }

    @Override
    public List<ByteBuffer> rangeQuery(String topic, int queueId, long startIndex, int num) {
        return null;
    }

    @Override
    public ByteBuffer get(String topic, int queueId, long startIndex) {
        return null;
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMaxOffsetInQueue();
        }
        return 0;
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            long resultOffset = logic.getOffsetInQueueByTime(timestamp, boundaryType);
            // Make sure the result offset is in valid range.
            resultOffset = Math.max(resultOffset, logic.getMinOffsetInQueue());
            resultOffset = Math.min(resultOffset, logic.getMaxOffsetInQueue());
            return resultOffset;
        }
        return 0;
    }

    private FileQueueLifeCycle getLifeCycle(String topic, int queueId) {
        // 查找或者创建消息队列
        return findOrCreateConsumeQueue(topic, queueId);
    }

    /**
     *
     */
    public boolean load(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.load();
    }

    /**
     * 加载 已经存在 的消息队列
     *
     */
    private boolean loadConsumeQueues(String storePath, CQType cqType) {
        File dirLogic = new File(storePath);
        File[] fileTopicList = dirLogic.listFiles();

        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                // topic 文件夹
                String topic = fileTopic.getName();
                // 队列名
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        // 验证消费队列类型 是否和 TopicConfig 一致
                        queueTypeShouldBe(topic, cqType);
                        // 根据类型创建 topic 的消息队列
                        ConsumeQueueInterface logic = createConsumeQueueByType(cqType, topic, queueId, storePath);
                        // 缓存消息队列信息 到 consumeQueueTable 中
                        this.putConsumeQueue(topic, queueId, logic);
                        // 消息队列加载方法，最终就是真正执行 mmap 文件内存映射
                        //
                        if (!this.load(logic)) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load {} all over, OK", cqType);

        return true;
    }

    /**
     * 根据类型创建不同类型的消息队列
     */
    private ConsumeQueueInterface createConsumeQueueByType(CQType cqType, String topic, int queueId, String storePath) {
        if (Objects.equals(CQType.SimpleCQ, cqType)) {
            /**
             * 创建普通 topic 的消费队列
             * ConsumeQueue 中创建 MappedFileQueue ，并根据消息存储的enableConsumeQueueExt配置，创建ConsumeQueueExt
             * topic 作为存储目录文件夹名，每个的队列对映一个文件，并建立内存映射
             */
            return new ConsumeQueue(
                topic,
                queueId,
                storePath,
                // 默认大小  30W * 20字节
                this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                this.messageStore);

        } else if (Objects.equals(CQType.BatchCQ, cqType)) {
            /**
             * 创建批量 topic 的消费队列
             */
            return new BatchConsumeQueue(
                topic,
                queueId,
                storePath,
                // 默认大小 30W * 46字节
                this.messageStoreConfig.getMapperFileSizeBatchConsumeQueue(),
                this.messageStore);
        } else {
            throw new RuntimeException(format("queue type %s is not supported.", cqType.toString()));
        }
    }

    /**
     * 验证消费队列类型 是否和 TopicConfig 一致
     */
    private void queueTypeShouldBe(String topic, CQType cqTypeExpected) {
        Optional<TopicConfig> topicConfig = this.messageStore.getTopicConfig(topic);

        CQType cqTypeActual = QueueTypeUtils.getCQType(topicConfig);

        if (!Objects.equals(cqTypeExpected, cqTypeActual)) {
            throw new RuntimeException(format("The queue type of topic: %s should be %s, but is %s", topic, cqTypeExpected, cqTypeActual));
        }
    }

    private ExecutorService buildExecutorService(BlockingQueue<Runnable> blockingQueue, String threadNamePrefix) {
        return ThreadUtils.newThreadPoolExecutor(
            this.messageStore.getBrokerConfig().getRecoverThreadPoolNums(),
            this.messageStore.getBrokerConfig().getRecoverThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            blockingQueue,
            new ThreadFactoryImpl(threadNamePrefix));
    }

    public void recover(ConsumeQueueInterface consumeQueue) {
        // 获取 队列 生命周期 对象：实际就是获取  ConsumeQueueInterface 接口的实现类： ConsumeQueue、BatchConsumeQueue
        // 消费队列是：某 topic 下的某 queueId
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        // 恢复：实际就是
        // 设置最大偏移量：在commitlog 的偏移量、
        // 设置mappedFileQueue属性：在多个文件中绝对偏移量
        fileQueueLifeCycle.recover();
    }

    @Override
    public Long getMaxPhyOffsetInConsumeQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMaxPhysicOffset();
        }
        return null;
    }

    @Override
    public long getMaxPhyOffsetInConsumeQueue() {
        long maxPhysicOffset = -1L;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }
        return maxPhysicOffset;
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    public void checkSelf(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.checkSelf();
    }

    @Override
    public void checkSelf() {
        for (Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> topicEntry : this.consumeQueueTable.entrySet()) {
            for (Map.Entry<Integer, ConsumeQueueInterface> cqEntry : topicEntry.getValue().entrySet()) {
                this.checkSelf(cqEntry.getValue());
            }
        }
    }

    @Override
    public boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.flush(flushLeastPages);
    }

    @Override
    public void destroy(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.destroy();
    }

    @Override
    public int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.deleteExpiredFile(minCommitLogPos);
    }

    public void truncateDirtyLogicFiles(ConsumeQueueInterface consumeQueue, long phyOffset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.truncateDirtyLogicFiles(phyOffset);
    }

    public void swapMap(ConsumeQueueInterface consumeQueue, int reserveNum, long forceSwapIntervalMs,
        long normalSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public void cleanSwappedMap(ConsumeQueueInterface consumeQueue, long forceCleanSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    @Override
    public boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileAvailable();
    }

    @Override
    public boolean isFirstFileExist(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileExist();
    }

    /**
     * 获取 某topic下 某队列 的信息
     */
    @Override
    public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
        //
        ConcurrentMap<Integer, ConsumeQueueInterface> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueueInterface> newMap = new ConcurrentHashMap<>(128);
            ConcurrentMap<Integer, ConsumeQueueInterface> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        // 创建消费队列
        ConsumeQueueInterface logic = map.get(queueId);
        if (logic != null) {
            return logic;
        }

        ConsumeQueueInterface newLogic;

        Optional<TopicConfig> topicConfig = this.messageStore.getTopicConfig(topic);
        // TODO maybe the topic has been deleted.
        if (Objects.equals(CQType.BatchCQ, QueueTypeUtils.getCQType(topicConfig))) {
            newLogic = new BatchConsumeQueue(
                topic,
                queueId,
                // user.home/store/batchconsumequeue
                getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.messageStoreConfig.getMapperFileSizeBatchConsumeQueue(),
                this.messageStore);
        } else {
            newLogic = new ConsumeQueue(
                topic,
                queueId,
                // user.home/store/consumequeue
                getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                this.messageStore);
        }

        ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);
        if (oldLogic != null) {
            logic = oldLogic;
        } else {
            logic = newLogic;
        }

        return logic;
    }

    public void setBatchTopicQueueTable(ConcurrentMap<String, Long> batchTopicQueueTable) {
        this.queueOffsetOperator.setBatchTopicQueueTable(batchTopicQueueTable);
    }

    public void updateQueueOffset(String topic, int queueId, long offset) {
        String topicQueueKey = topic + "-" + queueId;
        this.queueOffsetOperator.updateQueueOffset(topicQueueKey, offset);
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueueInterface consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    /**
     *
     * @param minPhyOffset  第一个 commit log 的文件名，代表 第一个 commit log 初始偏移量
     */
    @Override
    public void recoverOffsetTable(long minPhyOffset) {

        ConcurrentMap<String, Long> cqOffsetTable = new ConcurrentHashMap<>(1024);
        ConcurrentMap<String, Long> bcqOffsetTable = new ConcurrentHashMap<>(1024);
        // 遍历 topic 的所有Queue，如 topic0-1、topic0-2、topic1-1 每个队列下存在多个文件
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {

            for (ConsumeQueueInterface logic : maps.values()) {

                String key = logic.getTopic() + "-" + logic.getQueueId();
                // 普通消费队列：消息的数量
                //
                long maxOffsetInQueue = logic.getMaxOffsetInQueue();
                // 1、保存 topic 下每个队列的消息数量
                if (Objects.equals(CQType.BatchCQ, logic.getCQType())) {
                    bcqOffsetTable.put(key, maxOffsetInQueue);
                } else {
                    cqOffsetTable.put(key, maxOffsetInQueue);
                }

                // 2、修正最小偏移量：调整消费队列中 minLogicOffset 属性
                // 实际是 找到消费队列中，第一条大于 minPhyOffset 的消息，并在消费队列中 记录在消费队列中的偏移量
                this.correctMinOffset(logic, minPhyOffset);
            }
        }

        // Correct unSubmit consumeOffset
        // todo：待看
        if (messageStoreConfig.isDuplicationEnable() || messageStore.getBrokerConfig().isEnableControllerMode()) {
            compensateForHA(cqOffsetTable);
        }

        this.setTopicQueueTable(cqOffsetTable);
        this.setBatchTopicQueueTable(bcqOffsetTable);
    }
    private void compensateForHA(ConcurrentMap<String, Long> cqOffsetTable) {
        SelectMappedBufferResult lastBuffer = null;
        long startReadOffset = messageStore.getCommitLog().getConfirmOffset() == -1 ? 0 : messageStore.getCommitLog().getConfirmOffset();
        log.info("Correct unsubmitted offset...StartReadOffset = {}", startReadOffset);
        while ((lastBuffer = messageStore.selectOneMessageByOffset(startReadOffset)) != null) {
            try {
                if (lastBuffer.getStartOffset() > startReadOffset) {
                    startReadOffset = lastBuffer.getStartOffset();
                    continue;
                }

                ByteBuffer bb = lastBuffer.getByteBuffer();
                int magicCode = bb.getInt(bb.position() + 4);
                if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
                    startReadOffset += bb.getInt(bb.position());
                    continue;
                } else if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE) {
                    throw new RuntimeException("Unknown magicCode: " + magicCode);
                }

                lastBuffer.getByteBuffer().mark();
                DispatchRequest dispatchRequest = messageStore.getCommitLog().checkMessageAndReturnSize(lastBuffer.getByteBuffer(), true, messageStoreConfig.isDuplicationEnable(), true);
                if (!dispatchRequest.isSuccess())
                    break;
                lastBuffer.getByteBuffer().reset();

                MessageExt msg = MessageDecoder.decode(lastBuffer.getByteBuffer(), true, false, false, false, true);
                if (msg == null)
                    break;

                String key = msg.getTopic() + "-" + msg.getQueueId();
                cqOffsetTable.put(key, msg.getQueueOffset() + 1);
                startReadOffset += msg.getStoreSize();
                log.info("Correcting. Key:{}, start read Offset: {}", key, startReadOffset);
            } finally {
                if (lastBuffer != null)
                    lastBuffer.release();
            }
        }
    }

    @Override
    public void destroy() {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.destroy(logic);
            }
        }
    }

    @Override
    public void cleanExpired(long minCommitLogOffset) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> next = it.next();
            String topic = next.getKey();
            if (!TopicValidator.isSystemTopic(topic)) {
                ConcurrentMap<Integer, ConsumeQueueInterface> queueTable = next.getValue();
                Iterator<Map.Entry<Integer, ConsumeQueueInterface>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Map.Entry<Integer, ConsumeQueueInterface> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                            nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId(),
                            nextQT.getValue().getMaxPhysicOffset(),
                            nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                            "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                            topic,
                            nextQT.getKey(),
                            minCommitLogOffset,
                            maxCLOffsetInConsumeQueue);

                        removeTopicQueueTable(nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId());

                        this.destroy(nextQT.getValue());
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    @Override
    public void truncateDirty(long offsetToTruncate) {
        // 恢复 commit log 的时候，当消息队列的最大物理偏移量大于 commitLog 的时候，清除消息队列中超出的数据
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.truncateDirtyLogicFiles(logic, offsetToTruncate);
            }
        }
    }

    @Override
    public long getTotalSize() {
        long totalSize = 0;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                totalSize += logic.getTotalSize();
            }
        }
        return totalSize;
    }
}
