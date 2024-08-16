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

package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.store.DefaultMessageStore;

public class DefaultHAClient extends ServiceThread implements HAClient {

    /**
     * Report header buffer size. Schema: slaveMaxOffset. Format:
     *
     * <pre>
     * ┌───────────────────────────────────────────────┐
     * │                  slaveMaxOffset               │
     * │                    (8bytes)                   │
     * ├───────────────────────────────────────────────┤
     * │                                               │
     * │                  Report Header                │
     * </pre>
     * <p>
     */
    public static final int REPORT_HEADER_SIZE = 8;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    private final ByteBuffer reportOffset = ByteBuffer.allocate(REPORT_HEADER_SIZE);
    private SocketChannel socketChannel;
    private Selector selector;
    /**
     * last time that slave reads date from master.
     */
    private long lastReadTimestamp = System.currentTimeMillis();
    /**
     * last time that slave reports offset to master.
     */
    private long lastWriteTimestamp = System.currentTimeMillis();

    private long currentReportedOffset = 0;

    private int dispatchPosition = 0;
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private DefaultMessageStore defaultMessageStore;
    private volatile HAConnectionState currentState = HAConnectionState.READY;
    private FlowMonitor flowMonitor;

    public DefaultHAClient(DefaultMessageStore defaultMessageStore) throws IOException {
        this.selector = NetworkUtil.openSelector();
        this.defaultMessageStore = defaultMessageStore;
        this.flowMonitor = new FlowMonitor(defaultMessageStore.getMessageStoreConfig());
    }

    /**
     *  broker 启动的时候，initializeBrokerScheduledTasks 中设置 主节点地址
     */
    public void updateHaMasterAddress(final String newAddr) {
        String currentAddr = this.masterHaAddress.get();
        if (masterHaAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master ha address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }

    public void updateMasterAddress(final String newAddr) {
        String currentAddr = this.masterAddress.get();
        if (masterAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }

    public String getHaMasterAddress() {
        return this.masterHaAddress.get();
    }

    public String getMasterAddress() {
        return this.masterAddress.get();
    }

    private boolean isTimeToReportOffset() {
        long interval = defaultMessageStore.now() - this.lastWriteTimestamp;
        return interval > defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
    }

    private boolean reportSlaveMaxOffset(final long maxOffset) {
        this.reportOffset.position(0);
        // 8 字节
        this.reportOffset.limit(REPORT_HEADER_SIZE);
        this.reportOffset.putLong(maxOffset);
        this.reportOffset.position(0);
        this.reportOffset.limit(REPORT_HEADER_SIZE);

        // 当socket写入缓存没有空间时，尝试 3次写入
        for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
            try {
                this.socketChannel.write(this.reportOffset);
            } catch (IOException e) {
                log.error(this.getServiceName()
                    + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                return false;
            }
        }
        // 记录写入时间
        lastWriteTimestamp = this.defaultMessageStore.getSystemClock().now();
        // 判断是否已经写完
        return !this.reportOffset.hasRemaining();
    }

    /**
     * 重新分配ByteBuffer：使用备用的 bytebuffer 替换现有的
     * 将 剩余为持久化的消息放入 备用 buffer 中，清除 原有buffer
     */
    private void reallocateByteBuffer() {
        // 4M -
        int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
        // 还有剩余数据，将数据转移到 备用 buffer 中
        if (remain > 0) {
            this.byteBufferRead.position(this.dispatchPosition);

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
            this.byteBufferBackup.put(this.byteBufferRead);
        }

        // 转换 Buffer
        this.swapByteBuffer();

        this.byteBufferRead.position(remain);
        this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        this.dispatchPosition = 0;
    }

    private void swapByteBuffer() {
        ByteBuffer tmp = this.byteBufferRead;
        this.byteBufferRead = this.byteBufferBackup;
        this.byteBufferBackup = tmp;
    }

    /**
     * HA 客户端获取服务端消息数据，将接收到的消息全部保存到 commit log 中，每写一个消息就向主节点上报该从节点偏移量，不完整消息等待下次写入
     * todo：消息是多个吗？存在不完整的消息吗？
     * 服务端发送的消息，最大 32 kB，是数据片段，消息肯定存在不完整的情况，
     * 但是在客户端只会对消息完整的进行添加，剩余的数据等待下次接收后再进行处理
     */
    private boolean processReadEvent() {
        int readSizeZeroTimes = 0;
        // 创建 byteBuffer 后，肯定有剩余空间
        while (this.byteBufferRead.hasRemaining()) {
            try {
                /**
                 * 读取消息，最大 4M
                 * read 方法返回值：
                 * 大于0 ：代表读取的数据
                 * 0 ：当 byteBuffer 的空间已满，但是 socketChannel 还有数据
                 * -1：对端已经关闭了连接
                 */
                int readSize = this.socketChannel.read(this.byteBufferRead);
                if (readSize > 0) {
                    // 流控记录1s内的字节数
                    flowMonitor.addByteCountTransferred(readSize);
                    readSizeZeroTimes = 0;
                    //将接收到的消息全部保存到 commit log 中，每写一个消息就向主节点上报该从节点偏移量，不完整消息等待下次写入
                    boolean result = this.dispatchReadRequest();
                    if (!result) {
                        log.error("HAClient, dispatchReadRequest error");
                        return false;
                    }
                    lastReadTimestamp = System.currentTimeMillis();
                } else if (readSize == 0) {
                    if (++readSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    log.info("HAClient, processReadEvent read socket < 0");
                    return false;
                }
            } catch (IOException e) {
                log.info("HAClient, processReadEvent read socket exception", e);
                return false;
            }
        }

        return true;
    }

    /**
     * 将接收到的消息全部保存到 commit log 中
     * 每写一个消息就向主节点上报该从节点偏移量，不完整消息等待下次写入
     */
    private boolean dispatchReadRequest() {
        // 记录位置:byteBuffer 中数据的大小
        int readSocketPos = this.byteBufferRead.position();

        while (true) {
            // 剩余的保存到 commit log 的数据量
            int diff = this.byteBufferRead.position() - this.dispatchPosition;
            // 判断消息整体大小 大于 传输消息头的大小
            if (diff >= DefaultHAConnection.TRANSFER_HEADER_SIZE) {
                // 解析传输数据头：主节点commitLog 的偏移量、消息的大小
                long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                // 从节点的偏移量
                long slavePhyOffset = this.defaultMessageStore.getMaxPhyOffset();

                if (slavePhyOffset != 0) {
                    // 主从节点数据不一致
                    if (slavePhyOffset != masterPhyOffset) {
                        log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                            + slavePhyOffset + " MASTER: " + masterPhyOffset);
                        return false;
                    }
                }

                // 传输数据大小 符合要求，向从节点 commitLog 添加消息
                if (diff >= (DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize)) {
                    byte[] bodyData = byteBufferRead.array();

                    // 在 bodyData 中开始读取的位置。要去除传输数据头
                    int dataStart = this.dispatchPosition + DefaultHAConnection.TRANSFER_HEADER_SIZE;

                    // 向从节点 commitLog 添加消息
                    this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData, dataStart, bodySize);
                    // 重置 数据到最后
                    this.byteBufferRead.position(readSocketPos);
                    // 记录已经写入到 commit log 的数据位置
                    this.dispatchPosition += DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize;
                    //  保存当前从节点，已经保存的消息的偏移量，并且向主节点上报偏移量信息
                    if (!reportSlaveMaxOffsetPlus()) {
                        return false;
                    }
                    // 继续读取完整的消息
                    continue;
                }
            }

            // byteBufferRead 字节数组没有空间可以写入，使用备用数组替代
            // todo： 两个数组大小一样，替换作用是啥？ 情况：当byteBuffer写满，向commitlog 中写入数据并不会清空 byteBuffer，
            //  完整的消息都落完库落，但是可能消息不是完整的，将剩下的消息保存，等待下次存入
            //  这得取确认下，服务端发送的消息是一条还是多个？是否存在不完整消息
            if (!this.byteBufferRead.hasRemaining()) {
                this.reallocateByteBuffer();
            }

            break;
        }

        return true;
    }

    private boolean reportSlaveMaxOffsetPlus() {
        boolean result = true;
        long currentPhyOffset = this.defaultMessageStore.getMaxPhyOffset();
        if (currentPhyOffset > this.currentReportedOffset) {
            this.currentReportedOffset = currentPhyOffset;
            // 写完一个消息就向 主节点 上报，已经存储成功
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {
                // 上报失败
                this.closeMaster();
                log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
            }
        }

        return result;
    }

    public void changeCurrentState(HAConnectionState currentState) {
        log.info("change state to {}", currentState);
        this.currentState = currentState;
    }

    public boolean connectMaster() throws ClosedChannelException {
        if (null == socketChannel) {
            String addr = this.masterHaAddress.get();
            if (addr != null) {
                // 将 IP、端口 封装成 SocketAddress 对象
                SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
                // 连接服务端
                this.socketChannel = RemotingHelper.connect(socketAddress);
                if (this.socketChannel != null) {
                    // 监听 读事件
                    this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                    log.info("HAClient connect to master {}", addr);
                    // 改变 HA 客户端状态为 TRANSFER
                    this.changeCurrentState(HAConnectionState.TRANSFER);
                }
            }
            // 从节点 设置commitLog的最大偏移量
            this.currentReportedOffset = this.defaultMessageStore.getMaxPhyOffset();
            // 上次读取时间
            this.lastReadTimestamp = System.currentTimeMillis();
        }

        return this.socketChannel != null;
    }

    // 关闭与服务端连接
    public void closeMaster() {
        if (null != this.socketChannel) {
            try {

                SelectionKey sk = this.socketChannel.keyFor(this.selector);
                if (sk != null) {
                    sk.cancel();
                }

                this.socketChannel.close();

                this.socketChannel = null;

                log.info("HAClient close connection with master {}", this.masterHaAddress.get());
                // 改变状态
                this.changeCurrentState(HAConnectionState.READY);
            } catch (IOException e) {
                log.warn("closeMaster exception. ", e);
            }

            this.lastReadTimestamp = 0;
            this.dispatchPosition = 0;

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

            this.byteBufferRead.position(0);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        // 开启 客户端 数据发送流控
        this.flowMonitor.start();

        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case SHUTDOWN:
                        this.flowMonitor.shutdown(true);
                        return;
                    // 默认初始状态
                    case READY:
                        // 连接主机节点
                        if (!this.connectMaster()) {
                            log.warn("HAClient connect to master {} failed", this.masterHaAddress.get());
                            // 连接失败后，5s 后重试
                            this.waitForRunning(1000 * 5);
                        }
                        continue;
                    case TRANSFER:
                        // 将 主动向上报当前从节点的commit log 的最大偏移量
                        if (!transferFromMaster()) {
                            // 当写入缓存不足时候，进入这里，
                            // 关闭连接，将状态置为 READY ，之后重新连接主节点
                            closeMasterAndWait();
                            continue;
                        }
                        break;
                    default:
                        this.waitForRunning(1000 * 2);
                        continue;
                }
                long interval = this.defaultMessageStore.now() - this.lastReadTimestamp;
                // 间隔 大于 20s
                if (interval > this.defaultMessageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
                    log.warn("AutoRecoverHAClient, housekeeping, found this connection[" + this.masterHaAddress
                        + "] expired, " + interval);
                    this.closeMaster();
                    log.warn("AutoRecoverHAClient, master not response some time, so close connection");
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
                this.closeMasterAndWait();
            }
        }

        this.flowMonitor.shutdown(true);
        log.info(this.getServiceName() + " service end");
    }

    private boolean transferFromMaster() throws IOException {
        boolean result;
        // 判断是否大于 5s 的心跳时间
        if (this.isTimeToReportOffset()) {
            log.info("Slave report current offset {}", this.currentReportedOffset);
            // 主动向上报当前从节点的commit log 的最大偏移量
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {
                // 当socket 写入缓存不足，在尝试3次后也未能将数据写入 channel
                return false;
            }
        }

        // 获取服务端的发送的数据
        this.selector.select(1000);

        result = this.processReadEvent();
        if (!result) {
            return false;
        }
        // 上报从节点点当前最大的偏移量
        return reportSlaveMaxOffsetPlus();
    }

    public void closeMasterAndWait() {
        this.closeMaster();
        this.waitForRunning(1000 * 5);
    }

    public long getLastWriteTimestamp() {
        return this.lastWriteTimestamp;
    }

    public long getLastReadTimestamp() {
        return lastReadTimestamp;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        this.changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown();
        super.shutdown();

        closeMaster();
        try {
            this.selector.close();
        } catch (IOException e) {
            log.warn("Close the selector of AutoRecoverHAClient error, ", e);
        }
    }

    @Override
    public String getServiceName() {
        if (this.defaultMessageStore != null && this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return this.defaultMessageStore.getBrokerIdentity().getIdentifier() + DefaultHAClient.class.getSimpleName();
        }
        return DefaultHAClient.class.getSimpleName();
    }
}
