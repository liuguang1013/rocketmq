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

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageVersion;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.MessageExtEncoder.PutMessageThreadLocal;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.util.LibC;
import org.rocksdb.RocksDBException;

import sun.nio.ch.DirectBuffer;

/**
 * Store all metadata downtime for recovery, data protection reliability
 * 存储所有元数据停机恢复，数据保护可靠性
 */
public class CommitLog implements Swappable {
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194
    /**
     * 用来标识文件结尾
     */
    public final static int BLANK_MAGIC_CODE = -875286124;
    /**
     * CRC32 Format: [PROPERTY_CRC32 + NAME_VALUE_SEPARATOR + 10-digit fixed-length string + PROPERTY_SEPARATOR]
     */
    public static final int CRC32_RESERVED_LEN = MessageConst.PROPERTY_CRC32.length() + 1 + 10 + 1;
    protected final MappedFileQueue mappedFileQueue;
    protected final DefaultMessageStore defaultMessageStore;

    private final FlushManager flushManager;
    private final ColdDataCheckService coldDataCheckService;

    private final AppendMessageCallback appendMessageCallback;
    private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;

    /**
     * 消息已被成功处理并确认的标志。
     */
    protected volatile long confirmOffset = -1L;

    private volatile long beginTimeInLock = 0;

    protected final PutMessageLock putMessageLock;

    protected final TopicQueueLock topicQueueLock;

    private volatile Set<String> fullStorePaths = Collections.emptySet();

    private final FlushDiskWatcher flushDiskWatcher;

    protected int commitLogSize;

    private final boolean enabledAppendPropCRC;
    protected final MultiDispatch multiDispatch;

    public CommitLog(final DefaultMessageStore messageStore) {
        //user.home/store/commitlog
        String storePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        if (storePath.contains(MixAll.MULTI_PATH_SPLITTER)) {
            this.mappedFileQueue = new MultiPathMappedFileQueue(messageStore.getMessageStoreConfig(),
                messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                messageStore.getAllocateMappedFileService(), this::getFullStorePaths);
        } else {
            // 入参：存储地址、commitlog 默认1G、
            this.mappedFileQueue = new MappedFileQueue(storePath,
                messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                messageStore.getAllocateMappedFileService());
        }

        this.defaultMessageStore = messageStore;
        // 刷新管理器：flushCommitLogService、commitRealTimeService
        // todo：待看 数据落盘服务
        this.flushManager = new DefaultFlushManager();

        // todo：待看  判断消息是否在内存中,为统计使用
        this.coldDataCheckService = new ColdDataCheckService();
        // todo：待看  向 commitLog 提交数据最终会 回调到这个类
        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig());

        /**
         * 每个线程，向 commit Log 存储消息的时候，会在调用 putMessageThreadLocal.get() 方法，
         * 在当前线程中获取值，不存在的时候，执行initialValue 创建 PutMessageThreadLocal 对象
         */
        putMessageThreadLocal = new ThreadLocal<PutMessageThreadLocal>() {
            @Override
            protected PutMessageThreadLocal initialValue() {
                return new PutMessageThreadLocal(defaultMessageStore.getMessageStoreConfig());
            }
        };

        // 自旋锁、可重入锁。默认使用 可重入锁
        this.putMessageLock = messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage()
                ? new PutMessageReentrantLock() : new PutMessageSpinLock();
        // todo：待看 GroupCommitRequest 监控请求是否超时
        this.flushDiskWatcher = new FlushDiskWatcher();

        // 默认 32 个 ReentrantLock
        this.topicQueueLock = new TopicQueueLock(messageStore.getMessageStoreConfig().getTopicQueueLockNum());

        // 默认 1G
        this.commitLogSize = messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();

        // 用于控制是否启用附加消息属性的 CRC（循环冗余校验）  默认是 false
        this.enabledAppendPropCRC = messageStore.getMessageStoreConfig().isEnabledAppendPropCRC();

        this.multiDispatch = new MultiDispatch(defaultMessageStore);
    }

    public void setFullStorePaths(Set<String> fullStorePaths) {
        this.fullStorePaths = fullStorePaths;
    }

    public Set<String> getFullStorePaths() {
        return fullStorePaths;
    }

    public long getTotalSize() {
        return this.mappedFileQueue.getTotalFileSize();
    }

    public ThreadLocal<PutMessageThreadLocal> getPutMessageThreadLocal() {
        return putMessageThreadLocal;
    }

    public boolean load() {
        // 加载 commit Log 文件，并创建 mmap 内存映射，多个 DefaultMappedFile 最后缓存到 mappedFileQueue 的 list 中
        boolean result = this.mappedFileQueue.load();
        // MessageStoreConfig.dataReadAheadEnable 属性用于控制消息存储模块是否启用数据预读取功能。
        // 数据预读取功能是指在进行磁盘 I/O 操作时，提前将一些数据读入内存，从而减少后续读取时的磁盘 I/O 延迟，提高读取性能。
        if (result && !defaultMessageStore.getMessageStoreConfig().isDataReadAheadEnable()) {
            scanFileAndSetReadMode(LibC.MADV_RANDOM);
        }
        // 内存映射文件 自身检查，有序的多个文件名之间相差应该都是 1G
        this.mappedFileQueue.checkSelf();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        // 开启 commit log 数据落盘刷新服务：根据同步/异步刷盘模式，不同使用不同的服务
        // 在开启TransientStorePoolEnable（读写分离下），追平  committedPosition 和 writePosition 位置
        this.flushManager.start();
        log.info("start commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());

        // flushDiskWatcher设置为守护线程，
        // 当jvm中所有剩下的线程都是守护线程，那么JVM会终止并释放资源
        flushDiskWatcher.setDaemon(true);
        // 开启 GroupCommitRequest 请求是否超时
        flushDiskWatcher.start();

        if (this.coldDataCheckService != null) {
            // 开启冷数据检查服务，判断服务是否在内存中
            this.coldDataCheckService.start();
        }
    }

    public void shutdown() {
        this.flushManager.shutdown();
        log.info("shutdown commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        flushDiskWatcher.shutdown(true);
        if (this.coldDataCheckService != null) {
            this.coldDataCheckService.shutdown();
        }
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getFlushedWhere() {
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        return deleteExpiredFile(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, 0);
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately,
        final int deleteFileBatchMax
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, deleteFileBatchMax);
    }

    /**
     * Read CommitLog data, use data replication
     * 读取CommitLog数据，使用数据复制
     */
    public SelectMappedBufferResult getData(final long offset) {
        //
        return this.getData(offset, offset == 0);
    }


    /**
     *
     * @param offset 在 多个 commit log 中的绝对偏移量
     * @param returnFirstOnNotFound
     * @return
     */
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        // 获取commit log 映射文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // 根据offset  获取 mappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);

        if (mappedFile != null) {
            // 获取消息在 mappedFile 中的相对位置
            int pos = (int) (offset % mappedFileSize);
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    /**
     * 在 commit Log 中获取消息
     */
    public boolean getData(final long offset, final int size, final ByteBuffer byteBuffer) {
        // 1G
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();

        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.getData(pos, size, byteBuffer);
        }
        return false;
    }

    public List<SelectMappedBufferResult> getBulkData(final long offset, final int size) {
        List<SelectMappedBufferResult> bufferResultList = new ArrayList<>();

        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        int remainSize = size;
        long startOffset = offset;
        long maxOffset = this.getMaxOffset();
        if (offset + size > maxOffset) {
            remainSize = (int) (maxOffset - offset);
            log.warn("get bulk data size out of range, correct to max offset. offset: {}, size: {}, max: {}", offset, remainSize, maxOffset);
        }

        while (remainSize > 0) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(startOffset, startOffset == 0);
            if (mappedFile != null) {
                int pos = (int) (startOffset % mappedFileSize);
                int readableSize = mappedFile.getReadPosition() - pos;
                int readSize = Math.min(remainSize, readableSize);

                SelectMappedBufferResult bufferResult = mappedFile.selectMappedBuffer(pos, readSize);
                if (bufferResult == null) {
                    break;
                }
                bufferResultList.add(bufferResult);
                remainSize -= readSize;
                startOffset += readSize;
            }
        }

        return bufferResultList;
    }

    public SelectMappedFileResult getFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int size = (int) (mappedFile.getReadPosition() - offset % mappedFileSize);
            if (size > 0) {
                return new SelectMappedFileResult(size, mappedFile);
            }
        }
        return null;
    }

    //Create new mappedFile if not exits.
    public boolean getLastMappedFile(final long startOffset) {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
        if (null == lastMappedFile) {
            log.error("getLastMappedFile error. offset:{}", startOffset);
            return false;
        }

        return true;
    }

    /**
     * 恢复：设置commitlog 的逻辑队列的消息的刷新、消费、截断位置
     *
     *
     * When the normal exit, data recovery, all memory data have been flush
     * 当正常退出时，数据恢复，所有内存数据已被刷新
     *
     * @throws RocksDBException only in rocksdb mode
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        /**
         *  默认开启  CRC。
         * CRC（循环冗余校验，Cyclic Redundancy Check）是一种用于检测数据传输错误的校验算法。
         * RocketMQ 在消息存储和传输的过程中使用CRC校验码来确保消息的完整性和一致性。
         * 当消息被发送到RocketMQ的Broker时，消息的body部分会被计算出一个CRC校验值，这个值会附加到消息的元数据中一起存储。
         * 当消息被消费时，Broker会重新计算接收到的消息body的CRC值，并与存储的CRC值进行对比。
         * 如果两个CRC值匹配，则认为消息在传输过程中没有损坏；如果不匹配，则表示消息可能在存储或传输过程中发生了错误，此时Broker可以根据配置采取相应的错误处理措施。
         * 在RocketMQ的存储模型中，消息的元数据包含了一个名为bodyCRC的字段，它就是一个4字节的CRC校验码。
         */
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        // 默认是 false
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();

        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            // 从最后第三个文件开始恢复
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long lastValidMsgPhyOffset = this.getConfirmOffset();
            // normal recover doesn't require dispatching
            // 正常恢复不需要调度
            boolean doDispatch = false;
            while (true) {
                // 在 byteBuffer 中解析出消息数据，并封装到 DispatchRequest 中
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);

                int size = dispatchRequest.getMsgSize();
                // Normal data
                // 正常数据
                if (dispatchRequest.isSuccess() && size > 0) {
                    lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                    mappedFileOffset += size;
                    // 正常恢复，无需要分发请求
                    this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                // 到文件结束时，切换到下一个文件。
                // 由于返回0代表遇到了最后一个洞，因此不能将此包含在截断偏移量中
                else if (dispatchRequest.isSuccess() && size == 0) {
                    this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                // 中间文件读取错误，跳出循环
                else if (!dispatchRequest.isSuccess()) {
                    if (size > 0) {
                        log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                    }
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            processOffset += mappedFileOffset;

            if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                if (this.defaultMessageStore.getConfirmOffset() < this.defaultMessageStore.getMinPhyOffset()) {
                    log.error("confirmOffset {} is less than minPhyOffset {}, correct confirmOffset to minPhyOffset", this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMinPhyOffset());
                    this.defaultMessageStore.setConfirmOffset(this.defaultMessageStore.getMinPhyOffset());
                } else if (this.defaultMessageStore.getConfirmOffset() > processOffset) {
                    log.error("confirmOffset {} is larger than processOffset {}, correct confirmOffset to processOffset", this.defaultMessageStore.getConfirmOffset(), processOffset);
                    this.defaultMessageStore.setConfirmOffset(processOffset);
                }
            } else {
                // 设置 提交的 偏移量
                this.setConfirmOffset(lastValidMsgPhyOffset);
            }

            // Clear ConsumeQueue redundant data
            // 清除consumerqueue冗余数据：当消费队列的物理偏移量大于 commitLog 的最大偏移量
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }

            // 消息刷新位置
            this.mappedFileQueue.setFlushedWhere(processOffset);
            // 消息消费的位置
            this.mappedFileQueue.setCommittedWhere(processOffset);
            // 消息截断位置
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        } else {
            // Commitlog case files are deleted
            // commitlog不存在，消费队列文件没有存在意义，也删除
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.getQueueStore().destroy();
            this.defaultMessageStore.getQueueStore().loadAfterDestroy();
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC, final boolean checkDupInfo) {

        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     *
     * check the message and returns the message size
     * 解析传入的 commitLog 消息字节数组，并封装成DispatchRequest 对象
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean checkDupInfo, final boolean readBody) {

        try {
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MessageDecoder.MESSAGE_MAGIC_CODE:
                case MessageDecoder.MESSAGE_MAGIC_CODE_V2:
                    break;
                // 文件结束的标识符
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            MessageVersion messageVersion = MessageVersion.valueOfMagicCode(magicCode);

            byte[] bytesContent = new byte[totalSize];

            int bodyCRC = byteBuffer.getInt();

            int queueId = byteBuffer.getInt();

            int flag = byteBuffer.getInt();

            long queueOffset = byteBuffer.getLong();

            /**
             * todo：物理偏移量：是在 commit log 文件中的绝对偏移量吗？
             */
            long physicOffset = byteBuffer.getLong();

            int sysFlag = byteBuffer.getInt();

            long bornTimeStamp = byteBuffer.getLong();

            ByteBuffer byteBuffer1;
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            long storeTimestamp = byteBuffer.getLong();

            ByteBuffer byteBuffer2;
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            int reconsumeTimes = byteBuffer.getInt();

            long preparedTransactionOffset = byteBuffer.getLong();

            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);
                    // 循环冗余校验
                    if (checkCRC) {
                        /**
                         * When the forceVerifyPropCRC = false,
                         * use original bodyCrc validation.
                         */
                        if (!this.defaultMessageStore.getMessageStoreConfig().isForceVerifyPropCRC()) {
                            int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                            if (crc != bodyCRC) {
                                log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                                return new DispatchRequest(-1, false/* success */);
                            }
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            int topicLen = messageVersion.getTopicLength(byteBuffer);
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);

                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

                if (checkDupInfo) {
                    String dupInfo = propertiesMap.get(MessageConst.DUP_INFO);
                    if (null == dupInfo || dupInfo.split("_").length != 2) {
                        log.warn("DupInfo in properties check failed. dupInfo={}", dupInfo);
                        return new DispatchRequest(-1, false);
                    }
                }

                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.computeDeliverTimestamp(delayLevel,
                                storeTimestamp);
                        }
                    }
                }
            }

            if (checkCRC) {
                /**
                 * When the forceVerifyPropCRC = true,
                 * Crc verification needs to be performed on the entire message data (excluding the length reserved at the tail)
                 */
                // 默认 false
                if (this.defaultMessageStore.getMessageStoreConfig().isForceVerifyPropCRC()) {
                    int expectedCRC = -1;
                    if (propertiesMap != null) {
                        String crc32Str = propertiesMap.get(MessageConst.PROPERTY_CRC32);
                        if (crc32Str != null) {
                            expectedCRC = 0;
                            for (int i = crc32Str.length() - 1; i >= 0; i--) {
                                int num = crc32Str.charAt(i) - '0';
                                expectedCRC *= 10;
                                expectedCRC += num;
                            }
                        }
                    }
                    if (expectedCRC > 0) {
                        ByteBuffer tmpBuffer = byteBuffer.duplicate();
                        tmpBuffer.position(tmpBuffer.position() - totalSize);
                        tmpBuffer.limit(tmpBuffer.position() + totalSize - CommitLog.CRC32_RESERVED_LEN);
                        int crc = UtilAll.crc32(tmpBuffer);
                        if (crc != expectedCRC) {
                            log.warn(
                                "CommitLog#checkAndDispatchMessage: failed to check message CRC, expected "
                                    + "CRC={}, actual CRC={}", bodyCRC, crc);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    } else {
                        log.warn(
                            "CommitLog#checkAndDispatchMessage: failed to check message CRC, not found CRC in properties");
                        return new DispatchRequest(-1, false/* success */);
                    }
                }
            }

            int readLength = MessageExtEncoder.calMsgLength(messageVersion, sysFlag, bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                    "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                    totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            DispatchRequest dispatchRequest = new DispatchRequest(
                topic,
                queueId,
                physicOffset,
                totalSize,
                tagsCode,
                storeTimestamp,
                queueOffset,
                keys,
                uniqKey,
                sysFlag,
                preparedTransactionOffset,
                propertiesMap
            );

            // 设置 批量大小
            setBatchSizeIfNeeded(propertiesMap, dispatchRequest);

            return dispatchRequest;
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    private void setBatchSizeIfNeeded(Map<String, String> propertiesMap, DispatchRequest dispatchRequest) {
        if (null != propertiesMap && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_NUM)
                && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_BASE)) {
            dispatchRequest.setMsgBaseOffset(Long.parseLong(propertiesMap.get(MessageConst.PROPERTY_INNER_BASE)));
            dispatchRequest.setBatchSize(Short.parseShort(propertiesMap.get(MessageConst.PROPERTY_INNER_NUM)));
        }
    }

    // Fetch and compute the newest confirmOffset.
    // Even if it is just inited.
    public long getConfirmOffset() {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE && !this.defaultMessageStore.getRunningFlags().isFenced()) {
                if (((AutoSwitchHAService) this.defaultMessageStore.getHaService()).getLocalSyncStateSet().size() == 1
                    || !this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                    return this.defaultMessageStore.getMaxPhyOffset();
                }
                // First time it will compute the confirmOffset.
                if (this.confirmOffset < 0) {
                    setConfirmOffset(((AutoSwitchHAService) this.defaultMessageStore.getHaService()).computeConfirmOffset());
                    log.info("Init the confirmOffset to {}.", this.confirmOffset);
                }
            }
            return this.confirmOffset;
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return getMaxOffset();
        }
    }

    // Fetch the original confirmOffset's value.
    // Without checking and re-computing.
    public long getConfirmOffsetDirectly() {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE && !this.defaultMessageStore.getRunningFlags().isFenced()) {
                if (((AutoSwitchHAService) this.defaultMessageStore.getHaService()).getLocalSyncStateSet().size() == 1) {
                    return this.defaultMessageStore.getMaxPhyOffset();
                }
            }
            return this.confirmOffset;
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return getMaxOffset();
        }
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
        this.defaultMessageStore.getStoreCheckpoint().setConfirmPhyOffset(confirmOffset);
    }

    public long getLastFileFromOffset() {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (lastMappedFile != null) {
            if (lastMappedFile.isAvailable()) {
                return lastMappedFile.getFileFromOffset();
            }
        }

        return -1;
    }

    /**
     * @throws RocksDBException only in rocksdb mode
     */
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) throws RocksDBException {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            // 从最后一个文件开始查找，是否符合恢复的条件
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                // 通过判断首个消息的存储时间 小于 检查点的时间，认为可以从当前文件开始恢复
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long lastValidMsgPhyOffset = processOffset;
            long lastConfirmValidMsgPhyOffset = processOffset;
            // abnormal recover require dispatching
            // 需要分发调度：
            boolean doDispatch = true;
            while (true) {
                // 解析消息字节数组，并封装成DispatchRequest 对象
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
                int size = dispatchRequest.getMsgSize();

                if (dispatchRequest.isSuccess()) {
                    // Normal data
                    if (size > 0) {
                        // 上次有效的物理偏移量
                        lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                        mappedFileOffset += size;

                        // duplicationEnable 默认 false
                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()
                                // EnableControllerMode 默认 false
                                || this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                            if (dispatchRequest.getCommitLogOffset() + size <= this.defaultMessageStore.getCommitLog().getConfirmOffset()) {
                                this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                                lastConfirmValidMsgPhyOffset = dispatchRequest.getCommitLogOffset() + size;
                            }
                        } else {
                            // 调用
                            this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                        }
                    }
                    // Come the end of the file, switch to the next file
                    // Since the return 0 representatives met last hole, this can
                    // not be included in truncate offset
                    // 到文件结尾，开始处理下个文件
                    else if (size == 0) {
                        this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                        index++;
                        if (index >= mappedFiles.size()) {
                            // The current branch under normal circumstances should
                            // not happen
                            log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                            break;
                        } else {
                            mappedFile = mappedFiles.get(index);
                            byteBuffer = mappedFile.sliceByteBuffer();
                            processOffset = mappedFile.getFileFromOffset();
                            mappedFileOffset = 0;
                            log.info("recover next physics file, " + mappedFile.getFileName());
                        }
                    }
                } else {

                    if (size > 0) {
                        log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                    }

                    log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }

            // only for rocksdb mode
            this.getMessageStore().finishCommitLogDispatch();
            // 消息在commit log 中的绝对偏移量
            processOffset += mappedFileOffset;

            // 设置消息确认消费的偏移量
            if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                if (this.defaultMessageStore.getConfirmOffset() < this.defaultMessageStore.getMinPhyOffset()) {
                    log.error("confirmOffset {} is less than minPhyOffset {}, correct confirmOffset to minPhyOffset", this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMinPhyOffset());
                    this.defaultMessageStore.setConfirmOffset(this.defaultMessageStore.getMinPhyOffset());
                } else if (this.defaultMessageStore.getConfirmOffset() > lastConfirmValidMsgPhyOffset) {
                    log.error("confirmOffset {} is larger than lastConfirmValidMsgPhyOffset {}, correct confirmOffset to lastConfirmValidMsgPhyOffset", this.defaultMessageStore.getConfirmOffset(), lastConfirmValidMsgPhyOffset);
                    this.defaultMessageStore.setConfirmOffset(lastConfirmValidMsgPhyOffset);
                }
            } else {
                this.setConfirmOffset(lastValidMsgPhyOffset);
            }

            // Clear ConsumeQueue redundant data
            // 清除大于 commit log 的消费队列的信息
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }

            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        }
        // Commitlog case files are deleted
        else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.getQueueStore().destroy();
            this.defaultMessageStore.getQueueStore().loadAfterDestroy();
        }
    }

    public void truncateDirtyFiles(long phyOffset) {
        if (phyOffset <= this.getFlushedWhere()) {
            this.mappedFileQueue.setFlushedWhere(phyOffset);
        }

        if (phyOffset <= this.mappedFileQueue.getCommittedWhere()) {
            this.mappedFileQueue.setCommittedWhere(phyOffset);
        }

        this.mappedFileQueue.truncateDirtyFiles(phyOffset);
        if (this.confirmOffset > phyOffset) {
            this.setConfirmOffset(phyOffset);
        }
    }

    protected void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
        this.getMessageStore().onCommitLogAppend(msg, result, commitLogFile);
    }

    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) throws RocksDBException {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        // todo：此处获取的是文件头还是文件尾？
        // 查看 消息的 magicCode，判断是否属于正常消息
        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSITION);
        if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE
                && magicCode != MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isEnableRocksDBStore()) {
            final long maxPhyOffsetInConsumeQueue = this.defaultMessageStore.getQueueStore().getMaxPhyOffsetInConsumeQueue();
            long phyOffset = byteBuffer.getLong(MessageDecoder.MESSAGE_PHYSIC_OFFSET_POSITION);
            if (phyOffset <= maxPhyOffsetInConsumeQueue) {
                log.info("find check. beginPhyOffset: {}, maxPhyOffsetInConsumeQueue: {}", phyOffset, maxPhyOffsetInConsumeQueue);
                return true;
            }
        } else {
            int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
            int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornHostLength;
            long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
            if (0 == storeTimestamp) {
                return false;
            }

            // 默认 true
            if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
                    // 默认 false
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
                // 存储时间戳 小于 存储检查点最小索引时间戳
                if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                    log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                    return true;
                }
            } else {
                // 存储时间戳 小于 存储检查点最小时间戳
                if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                    log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                    return true;
                }
            }
        }

        return false;
    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    /**
     * 返回 Topic-QueueId
     */
    public String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
        // 将 StringBuilder 内容清空
        keyBuilder.setLength(0);
        keyBuilder.append(messageExt.getTopic());
        keyBuilder.append('-');
        keyBuilder.append(messageExt.getQueueId());
        return keyBuilder.toString();
    }

    public void setMappedFileQueueOffset(final long phyOffset) {
        this.mappedFileQueue.setFlushedWhere(phyOffset);
        this.mappedFileQueue.setCommittedWhere(phyOffset);
    }

    public void updateMaxMessageSize(PutMessageThreadLocal putMessageThreadLocal) {
        // dynamically adjust maxMessageSize, but not support DLedger mode temporarily
        // 动态调整maxMessageSize，但暂时不支持DLedger模式
        // 4M
        int newMaxMessageSize = this.defaultMessageStore.getMessageStoreConfig().getMaxMessageSize();

        if (newMaxMessageSize >= 10 && putMessageThreadLocal.getEncoder().getMaxMessageBodySize() != newMaxMessageSize) {
            putMessageThreadLocal.getEncoder().updateEncoderBufferCapacity(newMaxMessageSize);
        }
    }

    /**
     * 最终对 消息的
     * 先进行 topicQueueLock 加锁，设置消息在队列中的偏移量
     * 将消息编码成字节数组，设置到 MessageExtBrokerInner 消息的属性中
     * 再对 commit log 加锁，更新消息存储时间，保证消息顺序性，向commit log 最后一个文件中写入消息
     *
     * @param msg
     * @return
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        // 消息冗余开关，默认不开启，只会在主备同步时被复制到从 Broker。
        // 如果设置为 true，那么当一条消息被发送到 Broker 时，除了将消息存储在本地之外，还会尝试将这条消息复制到其他指定的 Broker 上，从而实现消息的冗余存储
        if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            // 设置存储时间戳
            msg.setStoreTimestamp(System.currentTimeMillis());
        }
        // Set the message body CRC (consider the most appropriate setting on the client)
        // 对消息体进行循环冗余校验的属性设置
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        if (enabledAppendPropCRC) {
            // delete crc32 properties if exist
            msg.deleteProperty(MessageConst.PROPERTY_CRC32);
        }
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        msg.setVersion(MessageVersion.MESSAGE_VERSION_V1);
        // 默认是 true
        boolean autoMessageVersionOnTopicLen = this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        // 当 topic 的长度大于 127时 设置为 v2
        if (autoMessageVersionOnTopicLen && topic.length() > Byte.MAX_VALUE) {
            msg.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }

        // 更新 最大消息大小
        PutMessageThreadLocal putMessageThreadLocal = this.putMessageThreadLocal.get();
        updateMaxMessageSize(putMessageThreadLocal);

        // Topic-QueueId
        String topicQueueKey = generateKey(putMessageThreadLocal.getKeyBuilder(), msg);
        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        // 默认 1
        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        // 是否需要处理 HA 高可用
        boolean needHandleHA = needHandleHA(msg);

        //todo ：待看
        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                // -1 means all ack in SyncStateSet
                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
            needAckNums = calcNeedAckNums(inSyncReplicas);
            if (needAckNums > inSyncReplicas) {
                // Tell the producer, don't have enough slaves to handle the send request
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }

        // 对 Topic-QueueId 字符串的hash 与 32 取余数，获取锁
        topicQueueLock.lock(topicQueueKey);
        try {

            boolean needAssignOffset = true;
            // 开启 冗余消息，并不是从节点
            if (defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()
                && defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
                // 不需要分配偏移量
                needAssignOffset = false;
            }

            if (needAssignOffset) {
                // 在 queueOffsetOperator.topicQueueTable 的缓存中，通过 key：Topic-QueueId ，获取缓存的队列偏移量，放入消息中
                defaultMessageStore.assignOffset(msg);
            }

            // 对消息的长度进行计算，检查，最终保存到  putMessageThreadLocal 的 Encoder 的 bytebuffer 属性
            PutMessageResult encodeResult = putMessageThreadLocal.getEncoder().encode(msg);
            // 不为空说明 encode 异常
            if (encodeResult != null) {
                return CompletableFuture.completedFuture(encodeResult);
            }

            msg.setEncodedBuff(putMessageThreadLocal.getEncoder().getEncoderBuffer());

            PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);

            putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
            try {
                //  System.currentTimeMillis()
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;

                // Here settings are stored timestamp, in order to ensure an orderly
                // global
                if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                    // 这里设置存储时间戳，以保证有序
                    msg.setStoreTimestamp(beginLockTimestamp);
                }

                if (null == mappedFile || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
                    // todo：涉及系统内核 待看
                    if (isCloseReadAhead()) {
                        setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                    }
                }
                if (null == mappedFile) {
                    log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }
                // 向commit log 最后一个文件中写入消息
                result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);

                switch (result.getStatus()) {
                    case PUT_OK:
                        // 什么也没做
                        onCommitLogAppend(msg, result, mappedFile);
                        break;
                    case END_OF_FILE:
                        onCommitLogAppend(msg, result, mappedFile);
                        unlockMappedFile = mappedFile;
                        // Create a new file, re-write the message
                        // 创建个新commit log，重新写入
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        if (isCloseReadAhead()) {
                            setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                        }
                        result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                        if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                            onCommitLogAppend(msg, result, mappedFile);
                        }
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                    default:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }
                // 锁定时间
                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                putMessageLock.unlock();
            }
            // Increase queue offset when messages are successfully written
            if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                this.defaultMessageStore.increaseOffset(msg, getMessageNum(msg));
            }
        } catch (RocksDBException e) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
        } finally {
            topicQueueLock.unlock(topicQueueKey);
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).add(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).add(result.getWroteBytes());

        // 处理 刷盘和 HA
        return handleDiskFlushAndHA(putMessageResult, msg, needAckNums, needHandleHA);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }

        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        boolean needHandleHA = needHandleHA(messageExtBatch);

        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                // -1 means all ack in SyncStateSet
                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
            needAckNums = calcNeedAckNums(inSyncReplicas);
            if (needAckNums > inSyncReplicas) {
                // Tell the producer, don't have enough slaves to handle the send request
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }

        messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V1);
        boolean autoMessageVersionOnTopicLen =
            this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        if (autoMessageVersionOnTopicLen && messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }

        //fine-grained lock instead of the coarse-grained
        PutMessageThreadLocal pmThreadLocal = this.putMessageThreadLocal.get();
        updateMaxMessageSize(pmThreadLocal);
        MessageExtEncoder batchEncoder = pmThreadLocal.getEncoder();

        String topicQueueKey = generateKey(pmThreadLocal.getKeyBuilder(), messageExtBatch);

        PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);
        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch, putMessageContext));

        topicQueueLock.lock(topicQueueKey);
        try {
            defaultMessageStore.assignOffset(messageExtBatch);

            putMessageLock.lock();
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;

                // Here settings are stored timestamp, in order to ensure an orderly
                // global
                messageExtBatch.setStoreTimestamp(beginLockTimestamp);

                if (null == mappedFile || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
                    if (isCloseReadAhead()) {
                        setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                    }
                }
                if (null == mappedFile) {
                    log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }

                result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                switch (result.getStatus()) {
                    case PUT_OK:
                        break;
                    case END_OF_FILE:
                        unlockMappedFile = mappedFile;
                        // Create a new file, re-write the message
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        if (isCloseReadAhead()) {
                            setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                        }
                        result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                    default:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }

                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                putMessageLock.unlock();
            }

            // Increase queue offset when messages are successfully written
            if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                this.defaultMessageStore.increaseOffset(messageExtBatch, (short) putMessageContext.getBatchSize());
            }
        } catch (RocksDBException e) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
        } finally {
            topicQueueLock.unlock(topicQueueKey);
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).add(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).add(result.getWroteBytes());

        return handleDiskFlushAndHA(putMessageResult, messageExtBatch, needAckNums, needHandleHA);
    }

    private int calcNeedAckNums(int inSyncReplicas) {
        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        if (this.defaultMessageStore.getMessageStoreConfig().isEnableAutoInSyncReplicas()) {
            needAckNums = Math.min(needAckNums, inSyncReplicas);
            needAckNums = Math.max(needAckNums, this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas());
        }
        return needAckNums;
    }

    private boolean needHandleHA(MessageExt messageExt) {
        // 无需等待消息存储成功
        if (!messageExt.isWaitStoreMsgOK()) {
            /*
              No need to sync messages that special config to extra broker slaves.
              @see MessageConst.PROPERTY_WAIT_STORE_MSG_OK
             */
            return false;
        }
        // 开启消息冗余，不需要处理 HA
        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return false;
        }

        if (BrokerRole.SYNC_MASTER != this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            // No need to check ha in async or slave broker
            return false;
        }

        return true;
    }

    private CompletableFuture<PutMessageResult> handleDiskFlushAndHA(PutMessageResult putMessageResult, MessageExt messageExt,
                                                                     int needAckNums, boolean needHandleHA) {

        // 处理刷盘
        CompletableFuture<PutMessageStatus> flushResultFuture = handleDiskFlush(putMessageResult.getAppendMessageResult(), messageExt);

        // replicaResultFuture 副本状态
        CompletableFuture<PutMessageStatus> replicaResultFuture;
        if (!needHandleHA) {
            replicaResultFuture = CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        } else {
            // 处理 高可用
            replicaResultFuture = handleHA(putMessageResult.getAppendMessageResult(), putMessageResult, needAckNums);
        }

        return flushResultFuture
                // 将两个任务进行合并
                .thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
                    if (flushStatus != PutMessageStatus.PUT_OK) {
                        putMessageResult.setPutMessageStatus(flushStatus);
                    }
                    if (replicaStatus != PutMessageStatus.PUT_OK) {
                        putMessageResult.setPutMessageStatus(replicaStatus);
                    }
                    return putMessageResult;
                });
    }

    /**
     * 向 磁盘刷新管理者 添加任务
     */
    private CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
        return this.flushManager.handleDiskFlush(result, messageExt);
    }

    private CompletableFuture<PutMessageStatus> handleHA(AppendMessageResult result,
                                                         PutMessageResult putMessageResult, int needAckNums) {
        if (needAckNums >= 0 && needAckNums <= 1) {
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }

        HAService haService = this.defaultMessageStore.getHaService();

        long nextOffset = result.getWroteOffset() + result.getWroteBytes();

        // Wait enough acks from different slaves
        GroupCommitRequest request = new GroupCommitRequest(nextOffset, this.defaultMessageStore.getMessageStoreConfig().getSlaveTimeout(), needAckNums);
        haService.putRequest(request);
        haService.getWaitNotifyObject().wakeupAll();
        return request.future();
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset() && offset + size <= this.getMaxOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
                    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
                    return result.getByteBuffer().getLong(msgStoreTimePos);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            // 判断 映射文件的状态是否可用
            if (mappedFile.isAvailable()) {
                // 返回 commitLog 最小的物理偏移量
                return mappedFile.getFileFromOffset();
            } else {
                // 返回下个文件的 文件名
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    /**
     * 将  commit log  中的消息封装到对象中
     * @param offset  消息 在 commit log 中的绝对偏移量
     * @param size 消息的大小
     * @return
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        // 1g
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();

        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            // 计算 消息在文件中的相对偏移量
            int pos = (int) (offset % mappedFileSize);
            SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(pos, size);
            if (null != selectMappedBufferResult) {
                // 设置消息在缓存中
                selectMappedBufferResult.setInCache(coldDataCheckService.isDataInPageCache(offset));
                return selectMappedBufferResult;
            }
        }
        return null;
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
        // 获取 可重入锁
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data, dataStart, dataLength);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    protected short getMessageNum(MessageExtBrokerInner msgInner) {
        short messageNum = 1;
        // IF inner batch, build batchQueueOffset and batchNum property.
        // 在 topic 配置信息中获取消息类型
        CQType cqType = getCqType(msgInner);

        // 批量消息
        if (MessageSysFlag.check(msgInner.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) || CQType.BatchCQ.equals(cqType)) {
            if (msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM) != null) {
                messageNum = Short.parseShort(msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM));
                messageNum = messageNum >= 1 ? messageNum : 1;
            }
        }

        return messageNum;
    }

    private CQType getCqType(MessageExtBrokerInner msgInner) {
        Optional<TopicConfig> topicConfig = this.defaultMessageStore.getTopicConfig(msgInner.getTopic());
        return QueueTypeUtils.getCQType(topicConfig);
    }

    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }


    /**
     *  在开启 读写分离的情况下，保证消息尽快落盘
     *  commitLog 更新 MappedFileQueue/MappedFile 提交位置
     *  追平  committedPosition 和 writePosition 位置
     *  并且 唤醒 刷新 commitlog 的 flushManager 线程 进行数据落盘
     *
     * 每 200 ms，刷新 commitLog，刷新数据页至少达到4个
     * 每 200 ，强制更新
     */
    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerIdentity().getIdentifier() + CommitRealTimeService.class.getSimpleName();
            }
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                // 默认： 200 ms
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();
                // 默认 4 个数据页，4* 4K
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();
                // 默认： 200 ms
                int commitDataThoroughInterval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    // 找到 提交位置 的 MappedFile，获取并更新 提交位置
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        // result = false means some data committed.
                        // result = false 以为有数据提交了，唤醒 刷新服务，commit log 数据落盘
                        this.lastCommitTimestamp = end;
                        CommitLog.this.flushManager.wakeUpFlush();
                    }
                    CommitLog.this.getMessageStore().getPerfCounter().flowOnce("COMMIT_DATA_TIME_MS", (int) (end - begin));
                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 正常关闭之前，更新commit log 的提交位置
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    /**
     * commitLog 异步 刷盘服务：强制刷盘，更新刷盘位置、时间
     *
     * 每 500 ms，刷新 commitLog，刷新数据页至少达到4个
     * 每 10s ，强制执行刷盘一次，持久化 commitLog
     */
    class FlushRealTimeService extends FlushCommitLogService {
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                // 默认是 true，是否定时刷新 commitLog
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();
                // 默认 500 ms，刷新 commitLog 间隔
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                // 默认刷新数据页数量 4 ， 大小 4 * 4K，强制刷盘标识
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();
                // 默认 10 s ，强制刷新时间间隔
                int flushPhysicQueueThoroughInterval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    // 达到强制刷新时间，需要物理数据页置为 0
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    /**
                     * 直接 线程 sleep 和 通过 CountDownLatch 挂起线程 有啥区别？
                     * 在 其他 服务中会进行唤醒 该服务的操作，如 CommitRealTimeService
                     * 如果直接使用 sleep ，其他服务使用 wakeup 方法也不能唤醒服务，达到定时执行该任务效果
                     */
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    // 数据刷盘
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);

                    // 更新 StoreCheckpoint 的 时间戳
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    long past = System.currentTimeMillis() - begin;
                    CommitLog.this.getMessageStore().getPerfCounter().flowOnce("FLUSH_DATA_TIME_MS", (int) past);
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            // 正常关机，确保 commitLog 刷新完
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getIdentifier() + FlushRealTimeService.class.getSimpleName();
            }
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    public static class GroupCommitRequest {
        private final long nextOffset;
        // Indicate the GroupCommitRequest result: true or false
        private final CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private volatile int ackNums = 1;
        private final long deadLine;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.deadLine = System.nanoTime() + (timeoutMillis * 1_000_000);
        }

        public GroupCommitRequest(long nextOffset, long timeoutMillis, int ackNums) {
            this(nextOffset, timeoutMillis);
            this.ackNums = ackNums;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        public int getAckNums() {
            return ackNums;
        }

        public long getDeadLine() {
            return deadLine;
        }

        public void wakeupCustomer(final PutMessageStatus status) {
            this.flushOKFuture.complete(status);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }
    }

    /**
     * GroupCommit Service
     * 同步刷盘服务
     * 当消息不存在 PROPERTY_WAIT_STORE_MSG_OK 属性 或者 属性为 true，创建GroupCommitRequest，通过任务执行刷盘；
     * 当消息存在 PROPERTY_WAIT_STORE_MSG_OK 属性 且 属性为 false，同步刷盘
     *
     * 每 10 ms 执行刷新 commitLog
     */
    class GroupCommitService extends FlushCommitLogService {
        /**
         * 在 FlushDiskType.SYNC_FLUSH 同步刷盘下，不存在 PROPERTY_WAIT_STORE_MSG_OK 属性 或者 属性为 true
         * 会创建 GroupCommitRequest 请求，添加到列表中
         */
        private volatile LinkedList<GroupCommitRequest> requestsWrite = new LinkedList<>();
        /**
         * 保存 requestsWrite 中的消息，转移过程加锁，后续处理请求，在 requestsRead 列表获取
         */
        private volatile LinkedList<GroupCommitRequest> requestsRead = new LinkedList<>();
        private final PutMessageSpinLock lock = new PutMessageSpinLock();

        public void putRequest(final GroupCommitRequest request) {
            lock.lock();
            try {
                // 添加写请求
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            // 唤醒当前线程
            this.wakeup();
        }

        /**
         * 请求转换：写请求 -> 读请求
         * 加锁防止并发问题，swapRequests 和 doCommit 串行，不会出现 遍历 requestsRead 列表过程中，内容发送变化
         */
        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        private void doCommit() {

            if (!this.requestsRead.isEmpty()) {
                /**
                 *   在多条单个消息到达 broker后，存储成功，创建GroupCommitRequest，加入队列
                 *   mappedFileQueue.flush 方法是直接处理，记录 FileFromOffset+读指针位置
                 *   有可能已经刷新过
                 */
                for (GroupCommitRequest req : this.requestsRead) {
                    // 判断是否已经 刷新过。
                    boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();

                    for (int i = 0; i < 1000 && !flushOK; i++) {
                        // 刷新
                        CommitLog.this.mappedFileQueue.flush(0);
                        flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                        if (flushOK) {

                            break;
                        } else {
                            // When transientStorePoolEnable is true, the messages in writeBuffer may not be committed
                            // to pageCache very quickly, and flushOk here may almost be false, so we can sleep 1ms to
                            // wait for the messages to be committed to pageCache.
                           // 当transientStorePoolEnable为true时，writeBuffer中的消息可能不会很快提交给pageCache，
                            // 而此处的flushOk可能几乎为false，因此我们可以休眠1ms来等待消息提交给pageCache。
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                    // 放入刷新结果
                    req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }

                // 更新 StoreCheckpoint 的 时间戳
                long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }

                // 清空 读请求队列
                this.requestsRead = new LinkedList<>();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                // 由于单个消息被设置为不同步刷新，因此它将进入此进程
                CommitLog.this.mappedFileQueue.flush(0);
            }
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 10 ms
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            // 正常情况下关机，等待请求的到来，然后刷新
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            this.swapRequests();
            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            //
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getIdentifier() + GroupCommitService.class.getSimpleName();
            }
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    class GroupCheckService extends FlushCommitLogService {
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<>();

        public boolean isAsyncRequestsFull() {
            return requestsWrite.size() > CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests() * 2;
        }

        public synchronized boolean putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
            boolean flag = this.requestsWrite.size() >
                CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests();
            if (flag) {
                log.info("Async requests {} exceeded the threshold {}", requestsWrite.size(),
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests());
            }

            return flag;
        }

        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        boolean flushOK = false;
                        for (int i = 0; i < 1000; i++) {
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                            if (flushOK) {
                                break;
                            } else {
                                try {
                                    Thread.sleep(1);
                                } catch (Throwable ignored) {

                                }
                            }
                        }
                        req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }

                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(1);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getIdentifier() + GroupCheckService.class.getSimpleName();
            }
            return GroupCheckService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;
        private final int crc32ReservedLength = CommitLog.CRC32_RESERVED_LEN;
        private final MessageStoreConfig messageStoreConfig;

        DefaultAppendMessageCallback(MessageStoreConfig messageStoreConfig) {
            //文件结尾 预留字节
            this.msgStoreItemMemory = ByteBuffer.allocate(END_FILE_MIN_BLANK_LENGTH);
            this.messageStoreConfig = messageStoreConfig;
        }

        public AppendMessageResult handlePropertiesForLmqMsg(ByteBuffer preEncodeBuffer,
            final MessageExtBrokerInner msgInner) {
            if (msgInner.isEncodeCompleted()) {
                return null;
            }

            multiDispatch.wrapMultiDispatch(msgInner);

            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

            final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            boolean needAppendLastPropertySeparator = enabledAppendPropCRC && propertiesData != null && propertiesData.length > 0
                && propertiesData[propertiesData.length - 1] != MessageDecoder.PROPERTY_SEPARATOR;

            final int propertiesLength = (propertiesData == null ? 0 : propertiesData.length) + (needAppendLastPropertySeparator ? 1 : 0) + crc32ReservedLength;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            int msgLenWithoutProperties = preEncodeBuffer.getInt(0);

            int msgLen = msgLenWithoutProperties + 2 + propertiesLength;

            // Exceeds the maximum message
            if (msgLen > this.messageStoreConfig.getMaxMessageSize()) {
                log.warn("message size exceeded, msg total size: " + msgLen + ", maxMessageSize: " + this.messageStoreConfig.getMaxMessageSize());
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // Back filling total message length
            preEncodeBuffer.putInt(0, msgLen);
            // Modify position to msgLenWithoutProperties
            preEncodeBuffer.position(msgLenWithoutProperties);

            preEncodeBuffer.putShort((short) propertiesLength);

            if (propertiesLength > crc32ReservedLength) {
                preEncodeBuffer.put(propertiesData);
            }

            if (needAppendLastPropertySeparator) {
                preEncodeBuffer.put((byte) MessageDecoder.PROPERTY_SEPARATOR);
            }
            // 18 CRC32
            preEncodeBuffer.position(preEncodeBuffer.position() + crc32ReservedLength);

            msgInner.setEncodeCompleted(true);

            return null;
        }

        /**
         * 将 MessageExtBrokerInner 消息 写入到  commit log 文件的 mappedByteBuffer 内存映射 中
         *
         * 生成消息id：ip、端口、开始写的位置
         *
         * @param fileFromOffset commit log 文件开始的
         * @param byteBuffer  commit log 文件的 mappedByteBuffer 内存映射，消息会写入到这里
         * @param maxBlank 文件空余的空间
         * @param msgInner  消息
         * @param putMessageContext 该方法未使用，处理批量的时候使用
         * @return AppendMessageResult  返回类型：1、文件空白空间，不能放下消息； 2、正常返回；3、处理MultiDispatch下返回值
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBrokerInner msgInner, PutMessageContext putMessageContext) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            ByteBuffer preEncodeBuffer = msgInner.getEncodedBuff();
            // enableMultiDispatch 默认是false，判断是不是多分发消息
            boolean isMultiDispatchMsg = messageStoreConfig.isEnableMultiDispatch() && CommitLog.isMultiDispatchMsg(msgInner);
            if (isMultiDispatchMsg) {
                // todo：待看
                AppendMessageResult appendMessageResult = handlePropertiesForLmqMsg(preEncodeBuffer, msgInner);
                if (appendMessageResult != null) {
                    return appendMessageResult;
                }
            }
            // 设置消息数组的limit
            final int msgLen = preEncodeBuffer.getInt(0);
            preEncodeBuffer.position(0);
            preEncodeBuffer.limit(msgLen);

            // PHY OFFSET
            // 计算消息开始写的 绝对偏移量
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // 消息id：将消息的ip、端口、开始写的位置，封装到字符串中
            Supplier<String> msgIdSupplier = () -> {
                int sysflag = msgInner.getSysFlag();
                int msgIdLen = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
                // 读模式 -> 切换到写模式
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer
                msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
                return UtilAll.bytes2string(msgIdBuffer.array());
            };

            // Record ConsumeQueue information
            // 记录消费队列的信息
            Long queueOffset = msgInner.getQueueOffset();

            // this msg maybe an inner-batch msg.
            // 消息的数量：判断是否是批量消息，在消息属性中获取消息数量
            short messageNum = getMessageNum(msgInner);

            // Transaction messages that require special handling
            // 对于事务消息，需要进行特殊处理
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the consume queue
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            // Determines whether there is sufficient free space
            // 判断commit log 是否有足够空间
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {

                this.msgStoreItemMemory.clear();
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);

                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset,
                    maxBlank, /* only wrote 8 bytes, but declare wrote maxBlank for compute write position */
                    msgIdSupplier, msgInner.getStoreTimestamp(),
                    queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            int pos = 4 + 4 + 4 + 4 + 4;
            // 6 QUEUEOFFSET
            preEncodeBuffer.putLong(pos, queueOffset);
            pos += 8;
            // 7 PHYSICALOFFSET
            preEncodeBuffer.putLong(pos, fileFromOffset + byteBuffer.position());
            int ipLen = (msgInner.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
            pos += 8 + 4 + 8 + ipLen;
            // refresh store time stamp in lock
            // 设置存储时间戳，是在锁中的，能保证存入顺序
            preEncodeBuffer.putLong(pos, msgInner.getStoreTimestamp());
            // 默认不开启 冗余校验
            if (enabledAppendPropCRC) {
                // 18 CRC32
                int checkSize = msgLen - crc32ReservedLength;
                ByteBuffer tmpBuffer = preEncodeBuffer.duplicate();
                tmpBuffer.limit(tmpBuffer.position() + checkSize);
                int crc32 = UtilAll.crc32(tmpBuffer);
                tmpBuffer.limit(tmpBuffer.position() + crc32ReservedLength);
                MessageDecoder.createCrc32(tmpBuffer, crc32);
            }

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            CommitLog.this.getMessageStore().getPerfCounter().startTick("WRITE_MEMORY_TIME_MS");
            // Write messages to the queue buffer
            // 将 消息保存到 mappedByteBuffer 中
            byteBuffer.put(preEncodeBuffer);
            CommitLog.this.getMessageStore().getPerfCounter().endTick("WRITE_MEMORY_TIME_MS");
            // 清空消息对象中的字节数组
            msgInner.setEncodedBuff(null);

            if (isMultiDispatchMsg) {
                CommitLog.this.multiDispatch.updateMultiQueueOffset(msgInner);
            }

            return new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgIdSupplier,
                msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills, messageNum);
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            Long queueOffset = messageExtBatch.getQueueOffset();
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            Supplier<String> msgIdSupplier = () -> {
                int msgIdLen = storeHostLength + 8;
                int batchCount = putMessageContext.getBatchSize();
                long[] phyPosArray = putMessageContext.getPhyPos();
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(messageExtBatch.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer

                StringBuilder buffer = new StringBuilder(batchCount * msgIdLen * 2 + batchCount - 1);
                for (int i = 0; i < phyPosArray.length; i++) {
                    msgIdBuffer.putLong(msgIdLen - 8, phyPosArray[i]);
                    String msgId = UtilAll.bytes2string(msgIdBuffer.array());
                    if (i != 0) {
                        buffer.append(',');
                    }
                    buffer.append(msgId);
                }
                return buffer.toString();
            };

            messagesByteBuff.mark();
            int index = 0;
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.msgStoreItemMemory.clear();
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdSupplier, messageExtBatch.getStoreTimestamp(),
                        beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                int pos = msgPos + 20;
                messagesByteBuff.putLong(pos, queueOffset);
                pos += 8;
                messagesByteBuff.putLong(pos, wroteOffset + totalMsgLen - msgLen);
                // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
                pos += 8 + 4 + 8 + bornHostLength;
                // refresh store time stamp in lock
                messagesByteBuff.putLong(pos, messageExtBatch.getStoreTimestamp());
                if (enabledAppendPropCRC) {
                    //append crc32
                    int checkSize = msgLen - crc32ReservedLength;
                    ByteBuffer tmpBuffer = messagesByteBuff.duplicate();
                    tmpBuffer.position(msgPos).limit(msgPos + checkSize);
                    int crc32 = UtilAll.crc32(tmpBuffer);
                    messagesByteBuff.position(msgPos + checkSize);
                    MessageDecoder.createCrc32(messagesByteBuff, crc32);
                }

                putMessageContext.getPhyPos()[index++] = wroteOffset + totalMsgLen - msgLen;
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdSupplier,
                messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);

            return result;
        }

    }

    /**
     * commit log 的数据刷盘管理器
     *
     * 区分 同步、异步 刷盘策略
     *
     */
    class DefaultFlushManager implements FlushManager {

        private final FlushCommitLogService flushCommitLogService;

        //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
        //如果启用了TransientStorePool，我们必须在固定的时间内将消息刷新到FileChannel
        private final FlushCommitLogService commitRealTimeService;

        public DefaultFlushManager() {
            //
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                // 同步刷盘。
                // 每 10 ms 执行刷新 commitLog
                // 当消息不存在 PROPERTY_WAIT_STORE_MSG_OK 属性 或者 属性为 true，创建GroupCommitRequest，通过任务执行刷盘；
                // 当消息存在 PROPERTY_WAIT_STORE_MSG_OK 属性 且 属性为 false，同步刷盘
                this.flushCommitLogService = new CommitLog.GroupCommitService();
            } else {
                // 默认刷盘策略：异步
                // 每 500 ms 执行刷新 commitLog，刷新数据页至少达到4个。每 10s ，强制执行刷盘一次，持久化 commitLog。
                this.flushCommitLogService = new CommitLog.FlushRealTimeService();
            }

            this.commitRealTimeService = new CommitLog.CommitRealTimeService();
        }

        /**
         * commitLog 数据刷盘
         * 在开启TransientStorePoolEnable（读写分离下），追平  committedPosition 和 writePosition 位置
         */
        @Override
        public void start() {
            // 默认异步刷新，FlushRealTimeService 服务开启
            // 每 500 ms，刷新 commitLog，刷新数据页至少达到4个，每 10s ，强制执行刷盘一次，持久化 commitLog
            this.flushCommitLogService.start();

            if (defaultMessageStore.isTransientStorePoolEnable()) {
                // 在开启 读写分离的情况下，保证消息尽快落盘，追平  committedPosition 和 writePosition 位置
                this.commitRealTimeService.start();
            }
        }

        public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult,
            MessageExt messageExt) {
            // Synchronization flush
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                if (messageExt.isWaitStoreMsgOK()) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    service.putRequest(request);
                    CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
                    PutMessageStatus flushStatus = null;
                    try {
                        flushStatus = flushOkFuture.get(CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        //flushOK=false;
                    }
                    if (flushStatus != PutMessageStatus.PUT_OK) {
                        log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags() + " client address: " + messageExt.getBornHostString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }
                } else {
                    service.wakeup();
                }
            }
            // Asynchronous flush
            else {
                if (!CommitLog.this.defaultMessageStore.isTransientStorePoolEnable()) {
                    flushCommitLogService.wakeup();
                } else {
                    commitRealTimeService.wakeup();
                }
            }
        }

        @Override
        public CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
            // 默认：异步刷新

            // Synchronization flush
            // todo： 同步待看
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                if (messageExt.isWaitStoreMsgOK()) {

                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(),
                                                        // 默认 5s
                                                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    flushDiskWatcher.add(request);
                    // 向 requestsWrite 列表中添加数据
                    service.putRequest(request);
                    return request.future();
                } else {

                    service.wakeup();
                    return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
                }
            }
            // Asynchronous flush
            else {
                // 根据是否开启 读写分离 ，唤醒 对映的刷新 commit log 的线程
                if (!CommitLog.this.defaultMessageStore.isTransientStorePoolEnable()) {
                    flushCommitLogService.wakeup();
                } else {
                    commitRealTimeService.wakeup();
                }
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        }

        @Override
        public void wakeUpFlush() {
            // now wake up flush thread.
            flushCommitLogService.wakeup();
        }

        @Override
        public void wakeUpCommit() {
            // now wake up commit log thread.
            commitRealTimeService.wakeup();
        }

        @Override
        public void shutdown() {
            if (defaultMessageStore.isTransientStorePoolEnable()) {
                this.commitRealTimeService.shutdown();
            }

            this.flushCommitLogService.shutdown();
        }

    }

    public int getCommitLogSize() {
        return commitLogSize;
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public MessageStore getMessageStore() {
        return defaultMessageStore;
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        this.getMappedFileQueue().swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public boolean isMappedFilesEmpty() {
        return this.mappedFileQueue.isMappedFilesEmpty();
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        this.getMappedFileQueue().cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    public FlushManager getFlushManager() {
        return flushManager;
    }

    public static boolean isMultiDispatchMsg(MessageExtBrokerInner msg) {
        return StringUtils.isNoneBlank(msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH)) && !msg.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
    }

    private boolean isCloseReadAhead() {
        return !MixAll.isWindows() && !defaultMessageStore.getMessageStoreConfig().isDataReadAheadEnable();
    }


    /**
     * 冷数据检查服务：方便数据统计
     * 每 1分钟，扫描 各个 mappedFile，通过 JNI 中的 LibC.INSTANCE.mincore 方法进行数据页是否在内存的判断，
     * 之后对数据页间隔 32 取样，在最后缓存数据到 pageCacheMap
     *
     */
    public class ColdDataCheckService extends ServiceThread {
        private final SystemClock systemClock = new SystemClock();
        private final ConcurrentHashMap<String, byte[]> pageCacheMap = new ConcurrentHashMap<>();
        private int pageSize = -1;
        private int sampleSteps = 32;

        public ColdDataCheckService() {
            sampleSteps = defaultMessageStore.getMessageStoreConfig().getSampleSteps();
            if (sampleSteps <= 0) {
                sampleSteps = 32;
            }
            // 根据操作系统，初始化数据页大小
            initPageSize();
            // 扫描 各个 mappedFile，进行数据页是否在内存的判断，之后取样，在最后缓存数据到 pageCacheMap
            scanFilesInPageCache();
        }

        @Override
        public String getServiceName() {
            return ColdDataCheckService.class.getSimpleName();
        }

        @Override
        public void run() {
            log.info("{} service started", this.getServiceName());
            while (!this.isStopped()) {
                try {
                    // 对系统、冷数据流控开关、冷数据浏览开关 判断
                    if (MixAll.isWindows() || !defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()
                            || !defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable()) {
                        pageCacheMap.clear();
                        // 3分钟
                        this.waitForRunning(180 * 1000);
                        continue;
                    } else {
                        // 1分钟
                        this.waitForRunning(defaultMessageStore.getMessageStoreConfig().getTimerColdDataCheckIntervalMs());
                    }

                    if (pageSize < 0) {
                        initPageSize();
                    }

                    long beginClockTimestamp = this.systemClock.now();
                    // 清除、重新构建缓存
                    scanFilesInPageCache();
                    long costTime = this.systemClock.now() - beginClockTimestamp;
                    log.info("[{}] scanFilesInPageCache-cost {} ms.", costTime > 30 * 1000 ? "NOTIFYME" : "OK", costTime);
                } catch (Throwable e) {
                    log.warn(this.getServiceName() + " service has e: {}", e);
                }
            }
            log.info("{} service end", this.getServiceName());
        }

        public boolean isDataInPageCache(final long offset) {
            // 默认 不开启 冷数据流控
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                return true;
            }
            if (pageSize <= 0 || sampleSteps <= 0) {
                return true;
            }
            // 数据不再冷数据
            if (!defaultMessageStore.checkInColdAreaByCommitOffset(offset, getMaxOffset())) {
                return true;
            }
            // 默认不开启冷数据浏览
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable()) {
                return false;
            }

            MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
            if (null == mappedFile) {
                return true;
            }
            byte[] bytes = pageCacheMap.get(mappedFile.getFileName());
            if (null == bytes) {
                return true;
            }

            // 获取 消息在 mappedFile 中的相对偏移量
            int pos = (int) (offset % defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog());
            int realIndex = pos / pageSize / sampleSteps;
            // 数据在内存中
            return bytes.length - 1 >= realIndex && bytes[realIndex] != 0;
        }

        /**
         * 扫描 各个 mappedFile，进行数据页是否在内存的判断，之后取样，在最后缓存数据到 pageCacheMap
         */
        private void scanFilesInPageCache() {

            if (MixAll.isWindows() || !defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()
                    || !defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable() || pageSize <= 0) {
                return;
            }

            try {
                log.info("pageCacheMap key size: {}", pageCacheMap.size());
                // 清理过期的 MappedFile
                clearExpireMappedFile();
                // 构建缓存
                mappedFileQueue.getMappedFiles().forEach(mappedFile -> {
                    // 检查 mappedFile 是否在 PageCache 中
                    byte[] pageCacheTable = checkFileInPageCache(mappedFile);
                    if (sampleSteps > 1) {

                        pageCacheTable = sampling(pageCacheTable, sampleSteps);
                    }
                    // 缓存 mappedFile 文件的数据页是否存在内存的取样数据
                    pageCacheMap.put(mappedFile.getFileName(), pageCacheTable);
                });
            } catch (Exception e) {
                log.error("scanFilesInPageCache exception", e);
            }
        }

        private void clearExpireMappedFile() {
            Set<String> currentFileSet = mappedFileQueue.getMappedFiles().stream().map(MappedFile::getFileName).collect(Collectors.toSet());
            pageCacheMap.forEach((key, value) -> {
                if (!currentFileSet.contains(key)) {
                    pageCacheMap.remove(key);
                    log.info("clearExpireMappedFile fileName: {}, has been clear", key);
                }
            });
        }

        /**
         * 在 pageCacheTable 中取样 ，取样步长 32
         */
        private byte[] sampling(byte[] pageCacheTable, int sampleStep) {
            // 对 sampleStep 32 进行向上取整
            byte[] sample = new byte[(pageCacheTable.length + sampleStep - 1) / sampleStep];
            for (int i = 0, j = 0; i < pageCacheTable.length && j < sample.length; i += sampleStep) {
                sample[j++] = pageCacheTable[i];
            }
            return sample;
        }

        private byte[] checkFileInPageCache(MappedFile mappedFile) {
            long fileSize = mappedFile.getFileSize();
            // 获取 MappedByteBuffer 地址
            final long address = ((DirectBuffer) mappedFile.getMappedByteBuffer()).address();
            // 计算 数据页 数量 ：(fileSize + this.pageSize - 1) 可以保证取整后，不丢数据
            int pageNums = (int) (fileSize + this.pageSize - 1) / this.pageSize;

            byte[] pageCacheRst = new byte[pageNums];
            // 使用 JNA (Java Native Access) 库调用 LibC.INSTANCE.mincore 方法的作用是获取指定内存区域的页表信息，
            // 以确定哪些页面已经被加载到物理内存中，哪些页面仍然在磁盘上或处于交换状态。
            // 如果页面在物理内存中，则对应的数组元素值为 1；如果页面不在物理内存中，则对应的数组元素值为 0。
            // 返回值 mincore 不为 0 ，代表调用方法失败
            int mincore = LibC.INSTANCE.mincore(new Pointer(address), new NativeLong(fileSize), pageCacheRst);

            if (mincore != 0) {
                log.error("checkFileInPageCache call the LibC.INSTANCE.mincore error, fileName: {}, fileSize: {}",
                    mappedFile.getFileName(), fileSize);
                // 调用失败，认为数据页都在缓存中
                for (int i = 0; i < pageNums; i++) {
                    pageCacheRst[i] = 1;
                }
            }
            return pageCacheRst;
        }

        private void initPageSize() {
            // coldDataFlowControlEnable 默认false
            if (pageSize < 0 && defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                try {
                    if (!MixAll.isWindows()) {
                        // 一般都是 4K
                        pageSize = LibC.INSTANCE.getpagesize();
                    } else {
                        defaultMessageStore.getMessageStoreConfig().setColdDataFlowControlEnable(false);
                        log.info("windows os, coldDataCheckEnable force setting to be false");
                    }
                    log.info("initPageSize pageSize: {}", pageSize);
                } catch (Exception e) {
                    defaultMessageStore.getMessageStoreConfig().setColdDataFlowControlEnable(false);
                    log.error("initPageSize error, coldDataCheckEnable force setting to be false ", e);
                }
            }
        }

        /**
         * this result is not high accurate.
         */
        public boolean isMsgInColdArea(String group, String topic, int queueId, long offset) {
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                return false;
            }
            try {
                ConsumeQueue consumeQueue = (ConsumeQueue) defaultMessageStore.findConsumeQueue(topic, queueId);
                if (null == consumeQueue) {
                    return false;
                }
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (null == bufferConsumeQueue || null == bufferConsumeQueue.getByteBuffer()) {
                    return false;
                }
                long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                return defaultMessageStore.checkInColdAreaByCommitOffset(offsetPy, getMaxOffset());
            } catch (Exception e) {
                log.error("isMsgInColdArea group: {}, topic: {}, queueId: {}, offset: {}",
                    group, topic, queueId, offset, e);
            }
            return false;
        }
    }

    public void scanFileAndSetReadMode(int mode) {
        if (MixAll.isWindows()) {
            log.info("windows os stop scanFileAndSetReadMode");
            return;
        }
        // 遍历 内存映射文件
        try {
            log.info("scanFileAndSetReadMode mode: {}", mode);
            mappedFileQueue.getMappedFiles().forEach(mappedFile -> {
                setFileReadMode(mappedFile, mode);
            });
        } catch (Exception e) {
            log.error("scanFileAndSetReadMode exception", e);
        }
    }

    /**
     * 这段代码的作用是调用本地（Native）方法 madvise，以建议操作系统如何管理一段内存区域。这在性能优化和资源管理中非常有用。
     */
    private int setFileReadMode(MappedFile mappedFile, int mode) {
        if (null == mappedFile) {
            log.error("setFileReadMode mappedFile is null");
            return -1;
        }
        // 设置文件预加载
        final long address = ((DirectBuffer) mappedFile.getMappedByteBuffer()).address();
        int madvise = LibC.INSTANCE.madvise(new Pointer(address), new NativeLong(mappedFile.getFileSize()), mode);
        if (madvise != 0) {
            log.error("setFileReadMode error fileName: {}, madvise: {}, mode:{}", mappedFile.getFileName(), madvise, mode);
        }
        return madvise;
    }

    public ColdDataCheckService getColdDataCheckService() {
        return coldDataCheckService;
    }
}
