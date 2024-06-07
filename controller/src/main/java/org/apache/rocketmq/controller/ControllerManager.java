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
package org.apache.rocketmq.controller;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.controller.impl.DLedgerController;
import org.apache.rocketmq.controller.impl.JRaftController;
import org.apache.rocketmq.controller.impl.heartbeat.RaftBrokerHeartBeatManager;
import org.apache.rocketmq.controller.metrics.ControllerMetricsManager;
import org.apache.rocketmq.controller.processor.ControllerRequestProcessor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.RoleChangeNotifyEntry;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.NotifyBrokerRoleChangedRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;

public class ControllerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

    private final ControllerConfig controllerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final BrokerHousekeepingService brokerHousekeepingService;
    private final Configuration configuration;
    private final RemotingClient remotingClient;
    private Controller controller;
    private final BrokerHeartbeatManager heartbeatManager;
    private ExecutorService controllerRequestExecutor;
    private BlockingQueue<Runnable> controllerRequestThreadPoolQueue;
    private final NotifyService notifyService;
    private ControllerMetricsManager controllerMetricsManager;

    public ControllerManager(ControllerConfig controllerConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) {

        this.controllerConfig = controllerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        //
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        // 配置文件进行合并保存
        this.configuration = new Configuration(log, this.controllerConfig, this.nettyServerConfig);
        // 从 controllerConfig中，通过反射获取字段的值
        this.configuration.setStorePathFromConfig(this.controllerConfig, "configStorePath");
        // 创建netty客户端，在初次发送请求的时候创建连接
        // todo:这是连接哪里的？
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        // 创建broker心跳管理器：默认模式、Raft模式
        this.heartbeatManager = BrokerHeartbeatManager.newBrokerHeartbeatManager(controllerConfig);
        // 创建通知服务
        this.notifyService = new NotifyService();
    }

    public boolean initialize() {
        // 阻塞队列
        this.controllerRequestThreadPoolQueue = new LinkedBlockingQueue<>(this.controllerConfig.getControllerRequestThreadPoolQueueCapacity());
        // 处理器请求线程池：
        this.controllerRequestExecutor = ThreadUtils.newThreadPoolExecutor(
            this.controllerConfig.getControllerThreadPoolNums(),
            this.controllerConfig.getControllerThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.controllerRequestThreadPoolQueue,
            new ThreadFactoryImpl("ControllerRequestExecutorThread_"));
        // 通知服务初始化：创建线程池
        this.notifyService.initialize();
        // 根据不同的 controller 类型创建 controller
        if (controllerConfig.getControllerType().equals(ControllerConfig.JRAFT_CONTROLLER)) {
            if (StringUtils.isEmpty(this.controllerConfig.getJraftConfig().getjRaftInitConf())) {
                throw new IllegalArgumentException("Attribute value jRaftInitConf of ControllerConfig is null or empty");
            }
            if (StringUtils.isEmpty(this.controllerConfig.getJraftConfig().getjRaftServerId())) {
                throw new IllegalArgumentException("Attribute value jRaftServerId of ControllerConfig is null or empty");
            }
            try {
                this.controller = new JRaftController(controllerConfig, this.brokerHousekeepingService);

                ((RaftBrokerHeartBeatManager) this.heartbeatManager)
                        .setController((JRaftController) this.controller);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            if (StringUtils.isEmpty(this.controllerConfig.getControllerDLegerPeers())) {
                throw new IllegalArgumentException("Attribute value controllerDLegerPeers of ControllerConfig is null or empty");
            }
            if (StringUtils.isEmpty(this.controllerConfig.getControllerDLegerSelfId())) {
                throw new IllegalArgumentException("Attribute value controllerDLegerSelfId of ControllerConfig is null or empty");
            }
            this.controller = new DLedgerController(this.controllerConfig, this.heartbeatManager::isBrokerActive,
                this.nettyServerConfig, this.nettyClientConfig, this.brokerHousekeepingService,
                new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo));
        }

        // Initialize the basic resources
        // 初始化基础资源：RaftBrokerHeartBeatManager什么也不做，DefaultBrokerHeartbeatManager初始化线程池
        this.heartbeatManager.initialize();

        // Register broker inactive listener
        // 注册 broker 失联监听者
        this.heartbeatManager.registerBrokerLifecycleListener(this::onBrokerInactive);

        this.controller.registerBrokerLifecycleListener(this::onBrokerInactive);
        // 注册各种类型请求的处理器，缓存到processorTable中
        registerProcessor();

        this.controllerMetricsManager = ControllerMetricsManager.getInstance(this);
        return true;
    }

    /**
     * When the heartbeatManager detects the "Broker is not active", we call this method to elect a master and do
     * something else.
     * 当heartbeatManager检测到“Broker不活跃”时，我们调用该方法来选择一个主节点并执行其他操作。
     *
     * @param clusterName The cluster name of this inactive broker
     * @param brokerName  The inactive broker name
     * @param brokerId    The inactive broker id, null means that the election forced to be triggered
     */
    private void onBrokerInactive(String clusterName, String brokerName, Long brokerId) {
        log.info("Controller Manager received broker inactive event, clusterName: {}, brokerName: {}, brokerId: {}", clusterName, brokerName, brokerId);

        // 判断当前节点是不是 leader
        if (controller.isLeaderState()) {
            if (brokerId == null) {
                // Means that force triggering election for this broker-set
                triggerElectMaster(brokerName);
                return;
            }
            // 获取 brokerName 的副本信息
            final CompletableFuture<RemotingCommand> replicaInfoFuture = controller.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName));

            replicaInfoFuture.whenCompleteAsync((replicaInfoResponse, err) -> {
                if (err != null || replicaInfoResponse == null) {
                    log.error("Failed to get replica-info for broker-set: {} when OnBrokerInactive", brokerName, err);
                    return;
                }
                final GetReplicaInfoResponseHeader replicaInfoResponseHeader = (GetReplicaInfoResponseHeader) replicaInfoResponse.readCustomHeader();
                // Not master broker offline
                // 不是 broker 的主节点 离线，不回触发 选举
                if (!brokerId.equals(replicaInfoResponseHeader.getMasterBrokerId())) {
                    log.warn("The broker with brokerId: {} in broker-set: {} has been inactive", brokerId, brokerName);
                    return;
                }
                // Trigger election
                // 触发选举
                triggerElectMaster(brokerName);
            });
        } else {
            log.warn("The broker with brokerId: {} in broker-set: {} has been inactive", brokerId, brokerName);
        }
    }

    private CompletableFuture<Boolean> triggerElectMaster0(String brokerName) {
        final CompletableFuture<RemotingCommand> electMasterFuture = controller.electMaster(ElectMasterRequestHeader.ofControllerTrigger(brokerName));
        return electMasterFuture.handleAsync((electMasterResponse, err) -> {
            if (err != null || electMasterResponse == null || electMasterResponse.getCode() != ResponseCode.SUCCESS) {
                log.error("Failed to trigger elect-master in broker-set: {}", brokerName, err);
                return false;
            }
            if (electMasterResponse.getCode() == ResponseCode.SUCCESS) {
                log.info("Elect a new master in broker-set: {} done, result: {}", brokerName, electMasterResponse);
                if (controllerConfig.isNotifyBrokerRoleChanged()) {
                    notifyBrokerRoleChanged(RoleChangeNotifyEntry.convert(electMasterResponse));
                }
                return true;
            }
            //default is false
            return false;
        });
    }

    private void triggerElectMaster(String brokerName) {
        int maxRetryCount = controllerConfig.getElectMasterMaxRetryCount();
        for (int i = 0; i < maxRetryCount; i++) {
            try {
                Boolean electResult = triggerElectMaster0(brokerName).get(3, TimeUnit.SECONDS);
                if (electResult) {
                    return;
                }
            } catch (Exception e) {
                log.warn("Failed to trigger elect-master in broker-set: {}, retryCount: {}", brokerName, i, e);
            }
        }
    }

    /**
     * Notify master and all slaves for a broker that the master role changed.
     */
    public void notifyBrokerRoleChanged(final RoleChangeNotifyEntry entry) {
        final BrokerMemberGroup memberGroup = entry.getBrokerMemberGroup();
        if (memberGroup != null) {
            final Long masterBrokerId = entry.getMasterBrokerId();
            String clusterName = memberGroup.getCluster();
            String brokerName = memberGroup.getBrokerName();
            if (masterBrokerId == null) {
                log.warn("Notify broker role change failed, because member group is not null but the new master brokerId is empty, entry:{}", entry);
                return;
            }
            // Inform all active brokers
            final Map<Long, String> brokerAddrs = memberGroup.getBrokerAddrs();
            brokerAddrs.entrySet().stream().filter(x -> this.heartbeatManager.isBrokerActive(clusterName, brokerName, x.getKey()))
                .forEach(x -> this.notifyService.notifyBroker(x.getValue(), entry));
        }
    }

    /**
     * Notify broker that there are roles-changing in controller
     *
     * @param brokerAddr target broker's address to notify
     * @param entry      role change entry
     */
    public void doNotifyBrokerRoleChanged(final String brokerAddr, final RoleChangeNotifyEntry entry) {
        if (StringUtils.isNoneEmpty(brokerAddr)) {
            log.info("Try notify broker {} that role changed, RoleChangeNotifyEntry:{}", brokerAddr, entry);
            final NotifyBrokerRoleChangedRequestHeader requestHeader = new NotifyBrokerRoleChangedRequestHeader(entry.getMasterAddress(), entry.getMasterBrokerId(),
                entry.getMasterEpoch(), entry.getSyncStateSetEpoch());
            final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFY_BROKER_ROLE_CHANGED, requestHeader);
            request.setBody(new SyncStateSet(entry.getSyncStateSet(), entry.getSyncStateSetEpoch()).encode());
            try {
                this.remotingClient.invokeOneway(brokerAddr, request, 3000);
            } catch (final Exception e) {
                log.error("Failed to notify broker {} that role changed", brokerAddr, e);
            }
        }
    }

    public void registerProcessor() {
        // 创建请求处理器
        final ControllerRequestProcessor controllerRequestProcessor = new ControllerRequestProcessor(this);
        // 获取controller的服务端
        RemotingServer controllerRemotingServer = this.controller.getRemotingServer();

        assert controllerRemotingServer != null;
        //注册实际就是 放入 processorTable 缓存中
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_ELECT_MASTER, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_REGISTER_BROKER, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_GET_REPLICA_INFO, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_GET_METADATA_INFO, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.BROKER_HEARTBEAT, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.UPDATE_CONTROLLER_CONFIG, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.GET_CONTROLLER_CONFIG, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CLEAN_BROKER_DATA, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_GET_NEXT_BROKER_ID, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_APPLY_BROKER_ID, controllerRequestProcessor, this.controllerRequestExecutor);
    }

    public void start() {
        this.controller.startup();
        this.heartbeatManager.start();
        this.remotingClient.start();
    }

    public void shutdown() {
        this.heartbeatManager.shutdown();
        this.controllerRequestExecutor.shutdown();
        this.notifyService.shutdown();
        this.controller.shutdown();
        this.remotingClient.shutdown();
    }

    public BrokerHeartbeatManager getHeartbeatManager() {
        return heartbeatManager;
    }

    public ControllerConfig getControllerConfig() {
        return controllerConfig;
    }

    public Controller getController() {
        return controller;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public BrokerHousekeepingService getBrokerHousekeepingService() {
        return brokerHousekeepingService;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    class NotifyService {
        private ExecutorService executorService;

        private Map<String/*brokerAddress*/, NotifyTask/*currentNotifyTask*/> currentNotifyFutures;

        public NotifyService() {
        }

        public void initialize() {
            this.executorService = Executors.newFixedThreadPool(3, new ThreadFactoryImpl("ControllerManager_NotifyService_"));
            this.currentNotifyFutures = new ConcurrentHashMap<>();
        }

        public void notifyBroker(String brokerAddress, RoleChangeNotifyEntry entry) {
            int masterEpoch = entry.getMasterEpoch();
            NotifyTask oldTask = this.currentNotifyFutures.get(brokerAddress);
            if (oldTask != null && masterEpoch > oldTask.getMasterEpoch()) {
                // cancel current future
                Future oldFuture = oldTask.getFuture();
                if (oldFuture != null && !oldFuture.isDone()) {
                    oldFuture.cancel(true);
                }
            }
            final NotifyTask task = new NotifyTask(masterEpoch, null);
            Runnable runnable = () -> {
                doNotifyBrokerRoleChanged(brokerAddress, entry);
                this.currentNotifyFutures.remove(brokerAddress, task);
            };
            this.currentNotifyFutures.put(brokerAddress, task);
            Future<?> future = this.executorService.submit(runnable);
            task.setFuture(future);
        }

        public void shutdown() {
            if (!this.executorService.isShutdown()) {
                this.executorService.shutdownNow();
            }
        }

        class NotifyTask extends Pair<Integer/*epochMaster*/, Future/*notifyFuture*/> {
            public NotifyTask(Integer masterEpoch, Future future) {
                super(masterEpoch, future);
            }

            public Integer getMasterEpoch() {
                return super.getObject1();
            }

            public Future getFuture() {
                return super.getObject2();
            }

            public void setFuture(Future future) {
                super.setObject2(future);
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(super.getObject1());
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) {
                    return true;
                }
                if (!(obj instanceof NotifyTask)) {
                    return false;
                }
                NotifyTask task = (NotifyTask) obj;
                return super.getObject1().equals(task.getObject1());
            }
        }
    }
}
