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

/*
  $Id: QueueData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.remoting.protocol.route;

public class QueueData implements Comparable<QueueData> {

    private String brokerName;
    /**
     * 读队列数
     * 实际是  topicConfig.getWriteQueueNums()
     * 默认 是 16
     *
     * updateTopicRouteInfoFromNameServer（）：默认 TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC ，生产者在 nameSrv 获取到 TopicRouteData 信息后，会对 readQueueNums 和生产者的配置值（默认是 4），取较小值
     */
    private int readQueueNums;
    /**
     * 写队列数
     * 实际是   topicConfig.getReadQueueNums()
     * 默认 是 16
     *
     * updateTopicRouteInfoFromNameServer（）：默认 TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC ，生产者在 nameSrv 获取到 TopicRouteData 信息后，会对 writeQueueNums 和生产者的配置值（默认是 4），取较小值
     */
    private int writeQueueNums;
    /**
     * 表示消息队列的权限
     * topicConfig.getPerm()
     * 但是 BrokerController#registerBrokerAll 方法，获取所有 topic 的配置后，
     *  如果broker 是不可读或写，会将 所有 topic 的配置也设置为不可读或写
     *
     *  写权限：对映生产者的消息写入
     *  读权限：对映消费者的消息消费
     */
    private int perm;

    /**
     * 默认是 0
     */
    private int topicSysFlag;

    public QueueData() {

    }

    // Deep copy QueueData
    public QueueData(QueueData queueData) {
        this.brokerName = queueData.brokerName;
        this.readQueueNums = queueData.readQueueNums;
        this.writeQueueNums = queueData.writeQueueNums;
        this.perm = queueData.perm;
        this.topicSysFlag = queueData.topicSysFlag;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public int getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + perm;
        result = prime * result + readQueueNums;
        result = prime * result + writeQueueNums;
        result = prime * result + topicSysFlag;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueueData other = (QueueData) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        if (perm != other.perm)
            return false;
        if (readQueueNums != other.readQueueNums)
            return false;
        if (writeQueueNums != other.writeQueueNums)
            return false;
        return topicSysFlag == other.topicSysFlag;
    }

    @Override
    public String toString() {
        return "QueueData [brokerName=" + brokerName + ", readQueueNums=" + readQueueNums
            + ", writeQueueNums=" + writeQueueNums + ", perm=" + perm + ", topicSysFlag=" + topicSysFlag
            + "]";
    }

    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
