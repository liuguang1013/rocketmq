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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class DefaultHAConnection implements HAConnection {

    /**
     * Transfer Header buffer size. Schema: physic offset and body size. Format:
     *
     * <pre>
     * ┌───────────────────────────────────────────────┬───────────────────────┐
     * │                  physicOffset                 │         bodySize      │
     * │                    (8bytes)                   │         (4bytes)      │
     * ├───────────────────────────────────────────────┴───────────────────────┤
     * │                                                                       │
     * │                           Transfer Header                             │
     * </pre>
     * <p>
     */
    public static final int TRANSFER_HEADER_SIZE = 8 + 4;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final DefaultHAService haService;
    private final SocketChannel socketChannel;
    private final String clientAddress;
    private WriteSocketService writeSocketService;
    private ReadSocketService readSocketService;
    /**
     * 服务端创建的 DefaultHAConnection 对象默认状态是 TRANSFER
     */
    private volatile HAConnectionState currentState = HAConnectionState.TRANSFER;
    private volatile long slaveRequestOffset = -1;
    private volatile long slaveAckOffset = -1;
    private FlowMonitor flowMonitor;

    /**
     *  创建 客户端连接：保存客户端 socketChannel 到 DefaultHAConnection 属性中
     *  设置客户端 socket 连接的属性：SO_LINGER、TCP_NODELAY、发送/接收缓冲大小
     *  创建读写服务，监听处理客户端 socketChannel 读/写事件
     *  创建流控监控器
     */
    public DefaultHAConnection(final DefaultHAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        // 客户端 socketChannel
        this.socketChannel = socketChannel;
        // 客户端地址
        this.clientAddress = this.socketChannel.socket().getRemoteSocketAddress().toString();
        // 设置为非阻塞 nio
        this.socketChannel.configureBlocking(false);
        // 设置 SO_LINGER 选项：
        // 关闭套接字时，会立即释放资源，不会等待任何剩余的数据发送完毕。
        // 关闭套接字时会等待 linger 指定的时间，以便发送任何未发送的数据。
        this.socketChannel.socket().setSoLinger(false, -1);
        // 设置 TCP_NODELAY 选项：
        // 当 TCP_NODELAY 被启用时（即 on 为 true），发送的数据会立即发送给对方，即使数据量很小。这有助于减少延迟，尤其是在交互式应用程序中，可以提高响应速度。
        // 当 TCP_NODELAY 被禁用时（即 on 为 false），TCP 可能会累积数据到一定量后再发送，这有助于减少网络带宽的使用，但在某些情况下可能会增加延迟。
        this.socketChannel.socket().setTcpNoDelay(true);

        // 设置 发送缓存区大小。
        // 当发送缓存区满后，对于阻塞模式，会等待直到有空间再次发送；对于非阻塞模式会立即返回或返回特定值，代表发送失败
        if (NettySystemConfig.socketSndbufSize > 0) {
            this.socketChannel.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
        }
        // 设置 接收缓存区大小。
        // 当接收缓冲区满后，对于 TCP 会丢弃数据包，触发重传机制，并触发窗口缩放，减小窗口大小
        if (NettySystemConfig.socketRcvbufSize > 0) {
            this.socketChannel.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
        }
        // 创建写服务：监听 OP_WRITE 写事件
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        // 创建读服务：监听 OP_READ 读事件
        this.readSocketService = new ReadSocketService(this.socketChannel);
        // 增加 HA 连接数量
        this.haService.getConnectionCount().incrementAndGet();
        // 创建流控监控器
        this.flowMonitor = new FlowMonitor(haService.getDefaultMessageStore().getMessageStoreConfig());
    }


    public void start() {
        changeCurrentState(HAConnectionState.TRANSFER);
        // 开启流控服务：每秒重置发送从节点的字节数
        this.flowMonitor.start();
        // 开启读取从节点已同步数据服务：在从节点发送数据中，获取从节点已同步的消息偏移量
        this.readSocketService.start();
        // 开启向从节点同步数据服务：不断向从节点同步 commit log 的数据
        this.writeSocketService.start();
    }

    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.flowMonitor.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void changeCurrentState(HAConnectionState currentState) {
        log.info("change state to {}", currentState);
        this.currentState = currentState;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public String getClientAddress() {
        return this.clientAddress;
    }

    @Override
    public long getSlaveAckOffset() {
        return slaveAckOffset;
    }

    public long getTransferredByteInSecond() {
        return this.flowMonitor.getTransferredByteInSecond();
    }

    public long getTransferFromWhere() {
        return writeSocketService.getNextTransferFromWhere();
    }

    /**
     * 在从节点发送数据中，获取从节点已同步的消息偏移量
     */
    class ReadSocketService extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        private final Selector selector;
        private final SocketChannel socketChannel;
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private int processPosition = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = NetworkUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        log.error("processReadEvent error");
                        break;
                    }

                    //
                    long interval = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + DefaultHAConnection.this.clientAddress + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            changeCurrentState(HAConnectionState.SHUTDOWN);

            // 停止当前服务
            this.makeStop();
            // 停止写入服务
            writeSocketService.makeStop();

            haService.removeConnection(DefaultHAConnection.this);

            DefaultHAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                log.error("", e);
            }

            flowMonitor.shutdown(true);

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + ReadSocketService.class.getSimpleName();
            }
            return ReadSocketService.class.getSimpleName();
        }

        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            if (!this.byteBufferRead.hasRemaining()) {
                // 变为读模式
                this.byteBufferRead.flip();
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 读取数据
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                       // 大于 8 字节
                        if ((this.byteBufferRead.position() - this.processPosition) >= DefaultHAClient.REPORT_HEADER_SIZE) {
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % DefaultHAClient.REPORT_HEADER_SIZE);
                            // 获取从节点的响应偏移量
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            // 记录处理位置
                            this.processPosition = pos;

                            DefaultHAConnection.this.slaveAckOffset = readOffset;
                            if (DefaultHAConnection.this.slaveRequestOffset < 0) {
                                DefaultHAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + DefaultHAConnection.this.clientAddress + "] request offset " + readOffset);
                            }

                            DefaultHAConnection.this.haService.notifyTransferSome(DefaultHAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.error("read socket[" + DefaultHAConnection.this.clientAddress + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * 不断向从节点发送 commitLog 的新数据
     */
    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;

        /**
         * 8+4 = 12 字节，请求头数据
         */
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(TRANSFER_HEADER_SIZE);
        private long nextTransferFromWhere = -1;
        private SelectMappedBufferResult selectMappedBufferResult;
        private boolean lastWriteOver = true;
        private long lastPrintTimestamp = System.currentTimeMillis();
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = NetworkUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            // 不断向从节点发送数据
            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);

                    // 初始值 -1，代表 ReadSocketService 服务还未获取到从节点偏移量
                    if (-1 == DefaultHAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    // 赋值 nextTransferFromWhere
                    if (-1 == this.nextTransferFromWhere) {
                        // 判断从节点的偏移量 是否为0 ，代表是够是从头开始进行数据同步
                        if (0 == DefaultHAConnection.this.slaveRequestOffset) {
                            // 主节点的commit log 的数据绝对偏移量
                            long masterOffset = DefaultHAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            masterOffset =
                                masterOffset
                                    - (masterOffset % DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }
                            // 主节点最后的 mappedFiled 的初始偏移量
                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            // 将从节点偏移量记录到属性
                            this.nextTransferFromWhere = DefaultHAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + DefaultHAConnection.this.clientAddress
                            + "], and slave request " + DefaultHAConnection.this.slaveRequestOffset);
                    }

                    // 向 客户端 socketChannel 写入数据头
                    if (this.lastWriteOver) {
                        // 计算上次发送的时间间隔
                        long interval = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        // 发送心跳
                        if (interval > DefaultHAConnection.this.haService.getDefaultMessageStore()
                                .getMessageStoreConfig().getHaSendHeartbeatInterval()) {

                            // Build Header
                            this.byteBufferHeader.position(0);
                            // 数据头，总共 8 + 4 = 12 字节
                            this.byteBufferHeader.limit(TRANSFER_HEADER_SIZE);
                            // 记录此次数据开始的偏移量： 主节点最后的 mappedFiled 的初始偏移量 或者 从节点的偏移量
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();
                            // 向客户端写入 心跳信息
                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        // 上次心跳信息未写完，继续写
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 获取需要同步的数据： 开始位置 到 读指针的位置
                    SelectMappedBufferResult selectResult =
                        DefaultHAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);

                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        // 消息数据 不能大于 32 kB
                        // todo： 消息的最大是 4M ，客户端也是按照最大 4M 接收的，这里面限制 32 kB 会不会使 消息不完整
                        if (size > DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }
                        // 通过流控计算可以传输的字节数
                        int canTransferMaxBytes = flowMonitor.canTransferMaxByteNum();
                        if (size > canTransferMaxBytes) {
                            if (System.currentTimeMillis() - lastPrintTimestamp > 1000) {
                                log.warn("Trigger HA flow control, max transfer speed {}KB/s, current speed: {}KB/s",
                                    String.format("%.2f", flowMonitor.maxTransferByteInSecond() / 1024.0),
                                    String.format("%.2f", flowMonitor.getTransferredByteInSecond() / 1024.0));
                                lastPrintTimestamp = System.currentTimeMillis();
                            }
                            size = canTransferMaxBytes;
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        this.nextTransferFromWhere += size;

                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(TRANSFER_HEADER_SIZE);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();
                        // 向从节点发送数据
                        this.lastWriteOver = this.transferData();
                    } else {
                        // commit log 中没有新数据写入，默认等待 100ms
                        DefaultHAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    DefaultHAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            // 关闭服务

            DefaultHAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            changeCurrentState(HAConnectionState.SHUTDOWN);

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(DefaultHAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                DefaultHAConnection.log.error("", e);
            }

            flowMonitor.shutdown(true);

            DefaultHAConnection.log.info(this.getServiceName() + " service end");
        }

        /**
         * 向从节点发送数据
         */
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            // 向 客户端 socketChannel 写入数据头
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    // 监控服务，添加传输数据量
                    flowMonitor.addByteCountTransferred(writeSize);
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + WriteSocketService.class.getSimpleName();
            }
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }

        public long getNextTransferFromWhere() {
            return nextTransferFromWhere;
        }
    }
}
