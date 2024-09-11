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

package org.apache.rocketmq.remoting.protocol;

public class RequestCode {

    public static final int SEND_MESSAGE = 10;

    public static final int PULL_MESSAGE = 11;

    public static final int QUERY_MESSAGE = 12;
    public static final int QUERY_BROKER_OFFSET = 13;
    public static final int QUERY_CONSUMER_OFFSET = 14;
    public static final int UPDATE_CONSUMER_OFFSET = 15;
    public static final int UPDATE_AND_CREATE_TOPIC = 17;
    /**
     * broker 启动，开启定时任务，当判断为从节点的时候，从主节点获取所有 topic 信息
     * 在 netty 服务端，使用 默认的 AdminBrokerProcessor 处理请求
     */
    public static final int GET_ALL_TOPIC_CONFIG = 21;
    public static final int GET_TOPIC_CONFIG_LIST = 22;

    public static final int GET_TOPIC_NAME_LIST = 23;

    public static final int UPDATE_BROKER_CONFIG = 25;

    public static final int GET_BROKER_CONFIG = 26;

    public static final int TRIGGER_DELETE_FILES = 27;

    public static final int GET_BROKER_RUNTIME_INFO = 28;
    public static final int SEARCH_OFFSET_BY_TIMESTAMP = 29;
    /**
     * 获取broker 主节点获取最大偏移量
     * 1、当消费者启动 RebalanceService 服务，消息是集群模式下，分配完 MessageQueue 后，更新 processQueueTable中
     * 添加新的 ProcessQueue 到缓存，构建 pullRequest 时，获取开始拉取位置：先从本地持久化文件获取，再从nameSrv
     */
    public static final int GET_MAX_OFFSET = 30;
    public static final int GET_MIN_OFFSET = 31;

    public static final int GET_EARLIEST_MSG_STORETIME = 32;

    public static final int VIEW_MESSAGE_BY_ID = 33;

    /**
     * 通过心跳 完成 生产/消费者组的信息 到 CustomerManager 注册
     *
     */
    public static final int HEART_BEAT = 34;

    public static final int UNREGISTER_CLIENT = 35;

    /**
     * ConsumeMessageConcurrentlyService 检查到消费者端，消息过期，将消息发回broker
     */
    public static final int CONSUMER_SEND_MSG_BACK = 36;

    public static final int END_TRANSACTION = 37;
    /**
     * 消费者启动的时候， RebalanceImpl 获取消费者列表，发送同步请求
     */
    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;

    public static final int CHECK_TRANSACTION_STATE = 39;

    /**
     * 1、broker 启动时，
     * new BrokerController 创建对象中，
     * new DefaultConsumerIdsChangeListener 创建对象时，
     * 定时任务 15 s 执行一次，遍历缓存 consumerChannelMap，
     * 通过 Broker2Client，实际是 NettyRemotingServer 给消费者组下的每个消费者发送该请求，只发送一次
     *
     * 2、消费者启动，向broker 发送心跳，携带消费者组信息，
     * 会向 ConsumerManager 中 registerConsumer注册消费者组信息，
     * 当新增/更新客户端channel 缓存  或者 更新 topic 信息的时候，发送 ConsumerGroupEvent.CHANGE 事件
     * DefaultConsumerIdsChangeListener 接收到事件，向所有的客户端发送 该请求
     *
     *
     */
    public static final int NOTIFY_CONSUMER_IDS_CHANGED = 40;

    public static final int LOCK_BATCH_MQ = 41;

    public static final int UNLOCK_BATCH_MQ = 42;
    /**
     * broker 启动，开启定时任务，当判断为从节点的时候，从主节点获取所有消费队列偏移量信息
     * 在 netty 服务端，使用 默认的 AdminBrokerProcessor 处理请求
     */
    public static final int GET_ALL_CONSUMER_OFFSET = 43;
    /**
     * broker 启动，开启定时任务，当判断为从节点的时候，从主节点获取ScheduleMessageService 对象的 json
     * 在 netty 服务端，使用 默认的 AdminBrokerProcessor 处理请求
     */
    public static final int GET_ALL_DELAY_OFFSET = 45;

    public static final int CHECK_CLIENT_CONFIG = 46;

    public static final int GET_CLIENT_CONFIG = 47;

    public static final int UPDATE_AND_CREATE_ACL_CONFIG = 50;

    public static final int DELETE_ACL_CONFIG = 51;

    public static final int GET_BROKER_CLUSTER_ACL_INFO = 52;

    public static final int UPDATE_GLOBAL_WHITE_ADDRS_CONFIG = 53;

    @Deprecated
    public static final int GET_BROKER_CLUSTER_ACL_CONFIG = 54;

    public static final int GET_TIMER_CHECK_POINT = 60;

    public static final int GET_TIMER_METRICS = 61;

    public static final int POP_MESSAGE = 200050;
    public static final int ACK_MESSAGE = 200051;
    public static final int BATCH_ACK_MESSAGE = 200151;
    public static final int PEEK_MESSAGE = 200052;
    public static final int CHANGE_MESSAGE_INVISIBLETIME = 200053;
    public static final int NOTIFICATION = 200054;
    public static final int POLLING_INFO = 200055;

    public static final int PUT_KV_CONFIG = 100;

    public static final int GET_KV_CONFIG = 101;

    public static final int DELETE_KV_CONFIG = 102;

    /**
     * broker 启动 brokerController#start()#registerBrokerAll#doRegisterBrokerAll
     * 最终 通过 brokerOuterAPI.registerBrokerAll 发送 REGISTER_BROKER 请求
     *
     * nameSrv 的 DefaultRequestProcessor 处理请求，
     * 最终进入到 RouteInfoManager#registerBroker 完成 broker 信息的缓存
     */
    public static final int REGISTER_BROKER = 103;

    public static final int UNREGISTER_BROKER = 104;
    /**
     * broker 启动 MQClientInstance#startScheduledTask()
     * broker 启动 DefaultMQProducerImpl#initTopicRoute()
     * 发送消息的时候查找 topic 对映的 TopicPublishInfo ，找不到也会调用
     *
     */
    public static final int GET_ROUTEINFO_BY_TOPIC = 105;

    public static final int GET_BROKER_CLUSTER_INFO = 106;
    public static final int UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;
    /**
     * broker 启动，开启定时任务，当判断为从节点的时候，从主节点获取 subscriptionGroupManager 对象的 json
     * 在 netty 服务端，使用 默认的 AdminBrokerProcessor 处理请求
     */
    public static final int GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;
    public static final int GET_TOPIC_STATS_INFO = 202;
    public static final int GET_CONSUMER_CONNECTION_LIST = 203;
    public static final int GET_PRODUCER_CONNECTION_LIST = 204;
    public static final int WIPE_WRITE_PERM_OF_BROKER = 205;

    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;

    public static final int DELETE_SUBSCRIPTIONGROUP = 207;
    public static final int GET_CONSUME_STATS = 208;

    public static final int SUSPEND_CONSUMER = 209;

    public static final int RESUME_CONSUMER = 210;
    public static final int RESET_CONSUMER_OFFSET_IN_CONSUMER = 211;
    public static final int RESET_CONSUMER_OFFSET_IN_BROKER = 212;

    public static final int ADJUST_CONSUMER_THREAD_POOL = 213;

    public static final int WHO_CONSUME_THE_MESSAGE = 214;

    public static final int DELETE_TOPIC_IN_BROKER = 215;

    public static final int DELETE_TOPIC_IN_NAMESRV = 216;
    public static final int REGISTER_TOPIC_IN_NAMESRV = 217;
    public static final int GET_KVLIST_BY_NAMESPACE = 219;

    public static final int RESET_CONSUMER_CLIENT_OFFSET = 220;

    public static final int GET_CONSUMER_STATUS_FROM_CLIENT = 221;

    public static final int INVOKE_BROKER_TO_RESET_OFFSET = 222;

    public static final int INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;

    public static final int QUERY_TOPIC_CONSUME_BY_WHO = 300;

    public static final int GET_TOPICS_BY_CLUSTER = 224;

    public static final int QUERY_TOPICS_BY_CONSUMER = 343;
    public static final int QUERY_SUBSCRIPTION_BY_CONSUMER = 345;

    public static final int REGISTER_FILTER_SERVER = 301;
    public static final int REGISTER_MESSAGE_FILTER_CLASS = 302;

    public static final int QUERY_CONSUME_TIME_SPAN = 303;

    public static final int GET_SYSTEM_TOPIC_LIST_FROM_NS = 304;
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;

    public static final int CLEAN_EXPIRED_CONSUMEQUEUE = 306;

    public static final int GET_CONSUMER_RUNNING_INFO = 307;

    public static final int QUERY_CORRECTION_OFFSET = 308;
    public static final int CONSUME_MESSAGE_DIRECTLY = 309;

    /**
     * broker 启动时，
     * createBrokerController
     * initialize()
     * recoverAndInitService()
     * registerMessageStoreHook（）
     * 向messageStore.setSendMessageBackHook(sendMessageBackHook);
     *
     * 遍历消息列表：给某个 broker 发送消息
     *
     * 1、发送同步简单消息，
     * MQClientAPIImpl#sendMessage：生产者发送消息
     */
    public static final int SEND_MESSAGE_V2 = 310;

    public static final int GET_UNIT_TOPIC_LIST = 311;

    public static final int GET_HAS_UNIT_SUB_TOPIC_LIST = 312;

    public static final int GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313;

    public static final int CLONE_GROUP_OFFSET = 314;

    public static final int VIEW_BROKER_STATS_DATA = 315;

    public static final int CLEAN_UNUSED_TOPIC = 316;

    public static final int GET_BROKER_CONSUME_STATS = 317;

    /**
     * update the config of name server
     */
    public static final int UPDATE_NAMESRV_CONFIG = 318;

    /**
     * get config from name server
     */
    public static final int GET_NAMESRV_CONFIG = 319;

    public static final int SEND_BATCH_MESSAGE = 320;

    public static final int QUERY_CONSUME_QUEUE = 321;

    public static final int QUERY_DATA_VERSION = 322;

    /**
     * resume logic of checking half messages that have been put in TRANS_CHECK_MAXTIME_TOPIC before
     */
    public static final int RESUME_CHECK_HALF_MESSAGE = 323;

    public static final int SEND_REPLY_MESSAGE = 324;

    public static final int SEND_REPLY_MESSAGE_V2 = 325;

    public static final int PUSH_REPLY_MESSAGE_TO_CLIENT = 326;

    public static final int ADD_WRITE_PERM_OF_BROKER = 327;

    public static final int GET_TOPIC_CONFIG = 351;

    public static final int GET_SUBSCRIPTIONGROUP_CONFIG = 352;
    public static final int UPDATE_AND_GET_GROUP_FORBIDDEN = 353;

    public static final int LITE_PULL_MESSAGE = 361;

    public static final int QUERY_ASSIGNMENT = 400;
    public static final int SET_MESSAGE_REQUEST_MODE = 401;
    /**
     * broker 启动，开启定时任务，当判断为从节点的时候，从主节点获取  MessageRequestModeManager 的 messageRequestModeMap
     * 在 netty 服务端，使用 默认的 AdminBrokerProcessor 处理请求
     */
    public static final int GET_ALL_MESSAGE_REQUEST_MODE = 402;

    public static final int UPDATE_AND_CREATE_STATIC_TOPIC = 513;

    public static final int GET_BROKER_MEMBER_GROUP = 901;

    public static final int ADD_BROKER = 902;

    public static final int REMOVE_BROKER = 903;

    public static final int BROKER_HEARTBEAT = 904;

    /**
     * 当broker超时未发送心跳时，定时任务扫描到后添加到 BatchUnregistrationService 的阻塞的队列
     * while(true) take任务后，移除RouteInfoManager中相关缓存，在是主节点并且配置允许下，发送该类请求到所有broker
     */
    public static final int NOTIFY_MIN_BROKER_ID_CHANGE = 905;

    public static final int EXCHANGE_BROKER_HA_INFO = 906;

    public static final int GET_BROKER_HA_STATUS = 907;

    public static final int RESET_MASTER_FLUSH_OFFSET = 908;

    public static final int GET_ALL_PRODUCER_INFO = 328;

    public static final int DELETE_EXPIRED_COMMITLOG = 329;

    /**
     * Controller code
     */
    public static final int CONTROLLER_ALTER_SYNC_STATE_SET = 1001;

    public static final int CONTROLLER_ELECT_MASTER = 1002;

    public static final int CONTROLLER_REGISTER_BROKER = 1003;

    public static final int CONTROLLER_GET_REPLICA_INFO = 1004;

    public static final int CONTROLLER_GET_METADATA_INFO = 1005;

    public static final int CONTROLLER_GET_SYNC_STATE_DATA = 1006;

    public static final int GET_BROKER_EPOCH_CACHE = 1007;

    public static final int NOTIFY_BROKER_ROLE_CHANGED = 1008;

    /**
     * update the config of controller
     */
    public static final int UPDATE_CONTROLLER_CONFIG = 1009;

    /**
     * get config from controller
     */
    public static final int GET_CONTROLLER_CONFIG = 1010;

    /**
     * clean broker data
     */
    public static final int CLEAN_BROKER_DATA = 1011;
    public static final int CONTROLLER_GET_NEXT_BROKER_ID = 1012;

    public static final int CONTROLLER_APPLY_BROKER_ID = 1013;
    public static final short BROKER_CLOSE_CHANNEL_REQUEST = 1014;
    public static final short CHECK_NOT_ACTIVE_BROKER_REQUEST = 1015;
    public static final short GET_BROKER_LIVE_INFO_REQUEST = 1016;
    public static final short GET_SYNC_STATE_DATA_REQUEST = 1017;
    public static final short RAFT_BROKER_HEART_BEAT_EVENT_REQUEST = 1018;

    public static final int UPDATE_COLD_DATA_FLOW_CTR_CONFIG = 2001;
    public static final int REMOVE_COLD_DATA_FLOW_CTR_CONFIG = 2002;
    public static final int GET_COLD_DATA_FLOW_CTR_INFO = 2003;
    public static final int SET_COMMITLOG_READ_MODE = 2004;
}
