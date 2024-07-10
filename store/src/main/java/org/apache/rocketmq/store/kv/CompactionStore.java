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
package org.apache.rocketmq.store.kv;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CleanupPolicy;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.CleanupPolicyUtils;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class CompactionStore {

    public static final String COMPACTION_DIR = "compaction";
    public static final String COMPACTION_LOG_DIR = "compactionLog";
    public static final String COMPACTION_CQ_DIR = "compactionCq";

    private final String compactionPath;
    private final String compactionLogPath;
    private final String compactionCqPath;
    private final DefaultMessageStore defaultMessageStore;
    private final CompactionPositionMgr positionMgr;
    private final ConcurrentHashMap<String, CompactionLog> compactionLogTable;
    private final ScheduledExecutorService compactionSchedule;
    private final int scanInterval = 30000;
    private final int compactionInterval;
    private final int compactionThreadNum;
    private final int offsetMapSize;
    private String masterAddr;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public CompactionStore(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.compactionLogTable = new ConcurrentHashMap<>();
        MessageStoreConfig config = defaultMessageStore.getMessageStoreConfig();
        // user.home/store/
        String storeRootPath = config.getStorePathRootDir();
        this.compactionPath = Paths.get(storeRootPath, COMPACTION_DIR).toString();
        this.compactionLogPath = Paths.get(compactionPath, COMPACTION_LOG_DIR).toString();
        this.compactionCqPath = Paths.get(compactionPath, COMPACTION_CQ_DIR).toString();
        // 创建对象，并加载文件，解析出 队列偏移脸属性
        this.positionMgr = new CompactionPositionMgr(compactionPath);

        // 线程数
        this.compactionThreadNum = Math.min(Runtime.getRuntime().availableProcessors(),
                // 6
                Math.max(1, config.getCompactionThreadNum()));

        // 线程池创建
        this.compactionSchedule = ThreadUtils.newScheduledThreadPool(this.compactionThreadNum,
            new ThreadFactoryImpl("compactionSchedule_"));

        // 100 M / 压缩线程数
        this.offsetMapSize = config.getMaxOffsetMapSize() / compactionThreadNum;

        // 压缩间隔 默认 15 分钟
        this.compactionInterval = defaultMessageStore.getMessageStoreConfig().getCompactionScheduleInternal();
    }


    /**
     * 加载 压缩日志文件
     */
    public void load(boolean exitOk) throws Exception {
        // user.home/store/compactionLog
        File logRoot = new File(compactionLogPath);
        File[] fileTopicList = logRoot.listFiles();
        if (fileTopicList != null) {
            // 遍历文件夹下，topic
            for (File fileTopic : fileTopicList) {
                if (!fileTopic.isDirectory()) {
                    continue;
                }

                // 遍历 topic 下队列文件
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        // todo：为啥此处消息队列是文件夹？
                        if (!fileQueueId.isDirectory()) {
                            continue;
                        }
                        try {
                            String topic = fileTopic.getName();
                            int queueId = Integer.parseInt(fileQueueId.getName());
                            // 判断 对映topic下的消息队列文件 是否是目录
                            if (Files.isDirectory(Paths.get(compactionCqPath, topic, String.valueOf(queueId)))) {

                                loadAndGetClog(topic, queueId);
                            } else {
                                log.error("{}:{} compactionLog mismatch with compactionCq", topic, queueId);
                            }
                        } catch (Exception e) {
                            log.error("load compactionLog {}:{} exception: ",
                                fileTopic.getName(), fileQueueId.getName(), e);
                            throw new Exception("load compactionLog " + fileTopic.getName()
                                + ":" + fileQueueId.getName() + " exception: " + e.getMessage());
                        }
                    }
                }
            }
        }
        log.info("compactionStore {}:{} load completed.", compactionLogPath, compactionCqPath);

        compactionSchedule.scheduleWithFixedDelay(this::scanAllTopicConfig, scanInterval, scanInterval, TimeUnit.MILLISECONDS);
        log.info("loop to scan all topicConfig with fixed delay {}ms", scanInterval);
    }

    private void scanAllTopicConfig() {
        log.info("start to scan all topicConfig");
        try {
            Iterator<Map.Entry<String, TopicConfig>> iterator = defaultMessageStore.getTopicConfigs().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, TopicConfig> it = iterator.next();
                TopicConfig topicConfig = it.getValue();
                CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(Optional.ofNullable(topicConfig));
                //check topic flag
                if (Objects.equals(policy, CleanupPolicy.COMPACTION)) {
                    for (int queueId = 0; queueId < topicConfig.getWriteQueueNums(); queueId++) {
                        loadAndGetClog(it.getKey(), queueId);
                    }
                }
            }
        } catch (Throwable ignore) {
            // ignore
        }
        log.info("scan all topicConfig over");
    }

    private CompactionLog loadAndGetClog(String topic, int queueId) {
        // 处理缓存值
        CompactionLog clog = compactionLogTable.compute(topic + "_" + queueId, (k, v) -> {
            // 缓存值不存在
            if (v == null) {
                try {
                    // 创建压缩日志对象
                    v = new CompactionLog(defaultMessageStore, this, topic, queueId);
                    // 执行加载方法
                    v.load(true);

                    // 默认15分钟
                    int randomDelay = 1000 + new Random(System.currentTimeMillis()).nextInt(compactionInterval);
                    // 定时任务中增加任务：
                    compactionSchedule.scheduleWithFixedDelay(v::doCompaction, compactionInterval + randomDelay, compactionInterval + randomDelay, TimeUnit.MILLISECONDS);
                } catch (IOException e) {
                    log.error("create compactionLog exception: ", e);
                    return null;
                }
            }
            return v;
        });
        return clog;
    }

    public void putMessage(String topic, int queueId, SelectMappedBufferResult smr) throws Exception {
        CompactionLog clog = loadAndGetClog(topic, queueId);

        if (clog != null) {
            clog.asyncPutMessage(smr);
        }
    }

    public void doDispatch(DispatchRequest dispatchRequest, SelectMappedBufferResult smr) throws Exception {
        CompactionLog clog = loadAndGetClog(dispatchRequest.getTopic(), dispatchRequest.getQueueId());

        if (clog != null) {
            clog.asyncPutMessage(smr.getByteBuffer(), dispatchRequest);
        }
    }

    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums, final int maxTotalMsgSize) {
        CompactionLog log = compactionLogTable.get(topic + "_" + queueId);
        if (log == null) {
            return GetMessageResult.NO_MATCH_LOGIC_QUEUE;
        } else {
            return log.getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize);
        }

    }

    public void flush(int flushLeastPages) {
        compactionLogTable.values().forEach(log -> log.flush(flushLeastPages));
    }

    public void flushLog(int flushLeastPages) {
        compactionLogTable.values().forEach(log -> log.flushLog(flushLeastPages));
    }

    public void flushCQ(int flushLeastPages) {
        compactionLogTable.values().forEach(log -> log.flushCQ(flushLeastPages));
    }

    public void updateMasterAddress(String addr) {
        this.masterAddr = addr;
    }

    public void shutdown() {
        // close the thread pool first
        compactionSchedule.shutdown();
        try {
            if (!compactionSchedule.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                List<Runnable> droppedTasks = compactionSchedule.shutdownNow();
                log.warn("compactionSchedule was abruptly shutdown. {} tasks will not be executed.", droppedTasks.size());
            }
        } catch (InterruptedException e) {
            log.warn("wait compaction schedule shutdown interrupted. ");
        }
        this.flush(0);
        positionMgr.persist();
    }

    public ScheduledExecutorService getCompactionSchedule() {
        return compactionSchedule;
    }

    public String getCompactionLogPath() {
        return compactionLogPath;
    }

    public String getCompactionCqPath() {
        return compactionCqPath;
    }

    public CompactionPositionMgr getPositionMgr() {
        return positionMgr;
    }

    public int getOffsetMapSize() {
        return offsetMapSize;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

}
