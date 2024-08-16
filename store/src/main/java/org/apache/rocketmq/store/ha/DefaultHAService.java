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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class DefaultHAService implements HAService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected final AtomicInteger connectionCount = new AtomicInteger(0);

    protected final List<HAConnection> connectionList = new LinkedList<>();

    protected AcceptSocketService acceptSocketService;

    protected DefaultMessageStore defaultMessageStore;

    protected WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    protected AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    protected GroupTransferService groupTransferService;

    protected HAClient haClient;

    protected HAConnectionStateNotificationService haConnectionStateNotificationService;

    public DefaultHAService() {
    }

    @Override
    public void init(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        // 创建 接收Socket。默认监听端口 10912
        this.acceptSocketService = new DefaultAcceptSocketService(defaultMessageStore.getMessageStoreConfig());
        // 创建 groupTransferService 服务
        this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
        // 从节点创建 HA 客户端
        if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
            // 创建的客户端默认状态是 READY
            this.haClient = new DefaultHAClient(this.defaultMessageStore);
        }
        // 创建 HA连接状态通知服务
        this.haConnectionStateNotificationService = new HAConnectionStateNotificationService(this, defaultMessageStore);
    }

    @Override
    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateHaMasterAddress(newAddr);
        }
    }

    @Override
    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    @Override
    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && masterPutWhere - this.push2SlaveMaxOffset.get() < this.defaultMessageStore
                .getMessageStoreConfig().getHaMaxGapNotInSync();
        return result;
    }

    public void notifyTransferSome(final long offset) {
        // 不断通过 CAS 设置 修改推送从节点的最大值
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                // 唤起 groupTransferService 线程
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    @Override
    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    @Override
    public void start() throws Exception {
        // 创建非阻塞 socket channel 和 自适应 selector ，开始监听从节点连接端口，处理 OP_ACCEPT 事件
        this.acceptSocketService.beginAccept();
        // 通过监听 channel 的 accept 事件，封装客户端（从节点） socketChannel 到 DefaultHAConnection 中
        // 同时对从节点socketChannel 的 READ/WRITE 事件监听，封装服务，并不断获取从节点已同步偏移量和同步主节点数据到从节点
        this.acceptSocketService.start();
        // GroupTransferService  每 10ms 执行一次，获取HA请求列表缓存遍历，不断的重试判断消息是否同步到从节点，直至消息同步从节点个数达到要求
        this.groupTransferService.start();
        // 对添加到服务的请求进行状态校验：
        //主节点：遍历客户端连接，查找与请求中地址匹配的客户端，状态是否与请求中预期的状态相同，不相同判断是否超时
        //从节点：判当前持有的HAClient 是否是请求期望的状态，不是的判断是否超时；
        this.haConnectionStateNotificationService.start();
        // 从节点 存在 HAClient，主节点不存在
        if (haClient != null) {
            // 开启从节点：从主节点获取数据，保存到commitLog
            this.haClient.start();
        }
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        this.haConnectionStateNotificationService.checkConnectionStateAndNotify(conn);
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    @Override
    public void shutdown() {
        if (this.haClient != null) {
            this.haClient.shutdown();
        }
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
        this.haConnectionStateNotificationService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    @Override
    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    @Override
    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    @Override
    public int inSyncReplicasNums(final long masterPutWhere) {
        int inSyncNums = 1;
        for (HAConnection conn : this.connectionList) {
            if (this.isInSyncSlave(masterPutWhere, conn)) {
                inSyncNums++;
            }
        }
        return inSyncNums;
    }

    protected boolean isInSyncSlave(final long masterPutWhere, HAConnection conn) {
        if (masterPutWhere - conn.getSlaveAckOffset() < this.defaultMessageStore.getMessageStoreConfig()
            .getHaMaxGapNotInSync()) {
            return true;
        }
        return false;
    }

    @Override
    public void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request) {
        this.haConnectionStateNotificationService.setRequest(request);
    }

    @Override
    public List<HAConnection> getConnectionList() {
        return connectionList;
    }

    @Override
    public HAClient getHAClient() {
        return this.haClient;
    }

    @Override
    public HARuntimeInfo getRuntimeInfo(long masterPutWhere) {
        HARuntimeInfo info = new HARuntimeInfo();

        if (BrokerRole.SLAVE.equals(this.getDefaultMessageStore().getMessageStoreConfig().getBrokerRole())) {
            info.setMaster(false);

            info.getHaClientRuntimeInfo().setMasterAddr(this.haClient.getHaMasterAddress());
            info.getHaClientRuntimeInfo().setMaxOffset(this.getDefaultMessageStore().getMaxPhyOffset());
            info.getHaClientRuntimeInfo().setLastReadTimestamp(this.haClient.getLastReadTimestamp());
            info.getHaClientRuntimeInfo().setLastWriteTimestamp(this.haClient.getLastWriteTimestamp());
            info.getHaClientRuntimeInfo().setTransferredByteInSecond(this.haClient.getTransferredByteInSecond());
            info.getHaClientRuntimeInfo().setMasterFlushOffset(this.defaultMessageStore.getMasterFlushedOffset());
        } else {
            info.setMaster(true);
            int inSyncNums = 0;

            info.setMasterCommitLogMaxOffset(masterPutWhere);

            for (HAConnection conn : this.connectionList) {
                HARuntimeInfo.HAConnectionRuntimeInfo cInfo = new HARuntimeInfo.HAConnectionRuntimeInfo();

                long slaveAckOffset = conn.getSlaveAckOffset();
                cInfo.setSlaveAckOffset(slaveAckOffset);
                cInfo.setDiff(masterPutWhere - slaveAckOffset);
                cInfo.setAddr(conn.getClientAddress().substring(1));
                cInfo.setTransferredByteInSecond(conn.getTransferredByteInSecond());
                cInfo.setTransferFromWhere(conn.getTransferFromWhere());

                boolean isInSync = this.isInSyncSlave(masterPutWhere, conn);
                if (isInSync) {
                    inSyncNums++;
                }
                cInfo.setInSync(isInSync);

                info.getHaConnectionInfo().add(cInfo);
            }
            info.setInSyncSlaveNums(inSyncNums);
        }
        return info;
    }

    class DefaultAcceptSocketService extends AcceptSocketService {

        public DefaultAcceptSocketService(final MessageStoreConfig messageStoreConfig) {
            super(messageStoreConfig);
        }

        @Override
        protected HAConnection createConnection(SocketChannel sc) throws IOException {
            // 创建 客户端连接，设置客户端 socket 连接的属性，
            return new DefaultHAConnection(DefaultHAService.this, sc);
        }

        @Override
        public String getServiceName() {
            if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return defaultMessageStore.getBrokerConfig().getIdentifier() + AcceptSocketService.class.getSimpleName();
            }
            return DefaultAcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    protected abstract class AcceptSocketService extends ServiceThread {
        private final SocketAddress socketAddressListen;
        private ServerSocketChannel serverSocketChannel;
        private Selector selector;

        private final MessageStoreConfig messageStoreConfig;

        public AcceptSocketService(final MessageStoreConfig messageStoreConfig) {
            this.messageStoreConfig = messageStoreConfig;
            // 默认监听端口：10912
            this.socketAddressListen = new InetSocketAddress(messageStoreConfig.getHaListenPort());
        }

        /**
         * Starts listening to slave connections.
         * 开始监听从节点连接。
         *
         * 创建 socket channel 和 selector ，开始监听从节点连接端口，处理 OP_ACCEPT 事件
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            // 创建一个新的服务器套接字通道，该通道用于监听客户端的连接请求
            this.serverSocketChannel = ServerSocketChannel.open();
            // 根据操作系统选择合适的 selector
            this.selector = NetworkUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            // channel 监听 10912 端口
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            // 早未指定监听端口的情况下，由系统选择监听端口，并将端口设置到 属性中
            if (0 == messageStoreConfig.getHaListenPort()) {
                messageStoreConfig.setHaListenPort(this.serverSocketChannel.socket().getLocalPort());
                log.info("OS picked up {} to listen for HA", messageStoreConfig.getHaListenPort());
            }
            //设置channel为非阻塞。 创建的通道默认是阻塞的，意味着对它的操作会阻塞调用线程直到操作完成。
            this.serverSocketChannel.configureBlocking(false);
            // 将选择器注册到 套接字通道 上，并监听 OP_ACCEPT 事件
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                if (null != this.serverSocketChannel) {
                    this.serverSocketChannel.close();
                }

                if (null != this.selector) {
                    this.selector.close();
                }
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 检测哪些注册的通道已经准备好进行I/O操作，
                    // 如果在这段时间内有通道准备好，选择器会返回，并且可以进一步处理这些通道。
                    // 如果没有通道准备好，则等待时间达到1000毫秒后也会返回。
                    // 返回值 代表准备好的通道的数量
                    this.selector.select(1000);
                    // 获取已经准备好的通道
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            // 判断当前channel Acceptable
                            if (k.isAcceptable()) {
                                // 接收客户端 socket 连接
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    DefaultHAService.log.info("HAService receive new connection, " + sc.socket().getRemoteSocketAddress());
                                    try {
                                        // 保存客户端 socketChannel 到 DefaultHAConnection 属性中，
                                        // 设置客户端 socket 连接的属性：SO_LINGER、TCP_NODELAY、发送/接收缓冲大小
                                        // 创建读写服务，监听处理客户端 socketChannel 读/写事件
                                        // 服务端创建的 DefaultHAConnection 对象默认状态是 TRANSFER
                                        HAConnection conn = createConnection(sc);
                                        // 开启流控服务：每秒重置发送从节点的字节数
                                        // 开启读取从节点已同步数据服务：在从节点发送数据中，获取从节点已同步的消息偏移量
                                        // 开启向从节点同步数据服务：不断向从节点同步 commit log 的数据
                                        conn.start();
                                        // 缓存客户端连接
                                        DefaultHAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * Create ha connection
         */
        protected abstract HAConnection createConnection(final SocketChannel sc) throws IOException;
    }
}
