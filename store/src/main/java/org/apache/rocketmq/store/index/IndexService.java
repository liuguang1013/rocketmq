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
package org.apache.rocketmq.store.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class IndexService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * Maximum times to attempt index file creation.
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    private final DefaultMessageStore defaultMessageStore;
    private final int hashSlotNum;
    private final int indexNum;
    /**
     * user.home/store/index
     */
    private final String storePath;
    private final ArrayList<IndexFile> indexFileList = new ArrayList<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        // 500 0000
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        // 500 0000 * 4
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        // user.home/store/index
        this.storePath =
            StorePathConfigHelper.getStorePathIndex(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 当 broker 启动，存在索引文件，加载索引文件
     * 删除大于 checkPoint 的索引消息时间戳 的文件
     */
    public boolean load(final boolean lastExitOK) {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    // 将文件封装都 IndexFile 对象中
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    // 加载文件，文件头封装到属性中
                    f.load();

                    // 上次不是正常退出
                    if (!lastExitOK) {
                        // 索引文件的最后时间戳 大于 checkPoint 的索引消息时间戳
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint().getIndexMsgTimestamp()) {
                            // 释放空间，删除文件
                            f.destroy(0);
                            continue;
                        }
                    }

                    LOGGER.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    LOGGER.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    LOGGER.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    public long getTotalSize() {
        if (indexFileList.isEmpty()) {
            return 0;
        }

        return (long) indexFileList.get(0).getFileSize() * indexFileList.size();
    }

    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            LOGGER.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            List<IndexFile> fileList = new ArrayList<>();
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                } else {
                    break;
                }
            }

            this.deleteExpiredFile(fileList);
        }
    }

    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        LOGGER.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            LOGGER.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<>(maxNum);

        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    IndexFile f = this.indexFileList.get(i - 1);
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    if (f.isTimeMatched(begin, end)) {

                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end);
                    }

                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }

    /**
     * 构建索引，获取 DispatchRequest 中 uniqKey、keys 属性，和 topic 组合成索引文件中的key，添加到 indexFile 中
     */
    public void buildIndex(DispatchRequest req) {
        //默认重试3次 获取最后一个索引文件
        //索引文件最后一个索引文件已经写满，创建新索引文件，对最后一个写满的文件进行强制刷盘，并刷新存储检查点信息
        IndexFile indexFile = retryGetAndCreateIndexFile();

        if (indexFile != null) {
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            String topic = msg.getTopic();
            String keys = msg.getKeys();
            // 消息 在commit log 中绝对偏移量 小于 消息在索引文件头中的物理偏移量
            if (msg.getCommitLogOffset() < endPhyOffset) {
                // 代表已经构建过索引，直接返回
                return;
            }

            // 判断消息类型：事务消息-回滚 不进行索引处理，其他情况正常进行
            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            // 获取消息唯一标识
            if (req.getUniqKey() != null) {
                // 向索引文件中 添加消息
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    LOGGER.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }

            // 对 keys 分别向索引文件中加数据
            if (keys != null && keys.length() > 0) {
                // 多个 key 之间使用， " " 分割
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            LOGGER.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            LOGGER.error("build index error, stop building index");
        }
    }

    /**
     * 向索引文件中写入消息索引， 文件写满后，创建新的文件，再次保存
     * @param indexFile 索引文件
     * @param msg 消息
     * @param idxKey topic + "#" + uniqKey  或者是 topic + "#" + keys          其中 keys 一个消息可能包含多个
     * @return
     */
    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {

        // 文件写满后，创建新的文件，再次保存
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            LOGGER.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");

            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }

            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        return indexFile;
    }

    /**
     * Retries to get or create index file.
     * 默认重试3次 获取最后一个索引文件
     * 索引文件最后一个索引文件已经写满，创建新索引文件，对最后一个写满的文件进行强制刷盘，并刷新存储检查点信息
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        // 默认的最大的重试创建次数 3次
        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            // 获取最后一个索引文件
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile) {
                break;
            }

            try {
                LOGGER.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted", e);
            }
        }
        // 消息存储服务：状态设置状态为错误
        if (null == indexFile) {
            this.defaultMessageStore.getRunningFlags().makeIndexFileError();
            LOGGER.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    /**
     * 获取最后一个索引文件
     * 索引文件最后一个索引文件已经写满，创建新索引文件，对最后一个写满的文件进行强制刷盘，并刷新存储检查点信息
     */
    public IndexFile getAndCreateLastIndexFile() {

        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        // 上次更新 结束的物理偏移量
        long lastUpdateEndPhyOffset = 0;
        // 上次更新索引时间
        long lastUpdateIndexTimestamp = 0;

        {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                // 获取最后一个索引文件
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {
                    indexFile = tmp;
                } else {
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }
        // 意味着 最后一个文件已经写满
        if (indexFile == null) {
            try {
                // user.home/store/index/
                String fileName =
                    this.storePath + File.separator
                        + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                // 创建 索引文件
                indexFile =
                    new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                        lastUpdateIndexTimestamp);
                this.readWriteLock.writeLock().lock();
                // 添加到 索引列表中
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                LOGGER.error("getLastIndexFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }

            // 创建新文件后，刷新 最后一个写满的文件
            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;

                Thread flushThread = new Thread(new AbstractBrokerRunnable(defaultMessageStore.getBrokerConfig()) {
                    @Override
                    public void run0() {
                        // 开始刷新：对最后一个写满的文件进行强制刷盘，并刷新存储检查点信息
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    /**
     * 当broker启动，上次不是正常退出，触发异常恢复，进行消息分发调度，调用CommitLogDispatcherBuildIndex构建索引时
     * 索引文件最后一个索引文件已经写满，创建新索引文件，对最后一个写满的文件进行强制刷盘，并刷新存储检查点信息
     */
    public void flush(final IndexFile f) {
        if (null == f) {
            return;
        }

        long indexMsgTimestamp = 0;
        // 文件已经写满的情况下，获取文件头中的 最后一次 放入索引信息时间
        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        // 对最后一个写满的文件，更新索引文件头信息，将数据强制刷到磁盘，并释放内存映射
        f.flush();

        // 刷新存储检查点信息
        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    public void shutdown() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                try {
                    f.shutdown();
                } catch (Exception e) {
                    LOGGER.error("shutdown " + f.getFileName() + " exception", e);
                }
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            LOGGER.error("shutdown exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }
}
