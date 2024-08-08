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
package org.apache.rocketmq.remoting.protocol.filter;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.util.Arrays;

public class FilterAPI {

    /**
     * 构建 tag 类型的订阅数据： 设置 Topic、订阅tag
     */
    public static SubscriptionData buildSubscriptionData(String topic, String subString) throws Exception {

        final SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);

        if (StringUtils.isEmpty(subString) || subString.equals(SubscriptionData.SUB_ALL)) {
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
            return subscriptionData;
        }
        // 订阅多个 tag，tag 通过 || 分割
        String[] tags = subString.split("\\|\\|");
        if (tags.length > 0) {
            Arrays.stream(tags).map(String::trim).filter(tag -> !tag.isEmpty()).forEach(tag -> {
                subscriptionData.getTagsSet().add(tag);
                subscriptionData.getCodeSet().add(tag.hashCode());
            });
        } else {
            throw new Exception("subString split error");
        }

        return subscriptionData;
    }

    public static SubscriptionData buildSubscriptionData(String topic, String subString, String expressionType) throws Exception {
        final SubscriptionData subscriptionData = buildSubscriptionData(topic, subString);
        if (StringUtils.isNotBlank(expressionType)) {
            subscriptionData.setExpressionType(expressionType);
        }
        return subscriptionData;
    }

    /**
     * 构建订阅数据信息：封装 topic 、tag
     * @param topic
     * @param subString 订阅内容：tag 类型：保存为具体的 tag，如 "*" ，"tag1||tag2||tag3"
     * @param type  requestHeader.getExpressionType()  可能是 tag 类型，也有可能是表达式类型
     */
    public static SubscriptionData build(final String topic, final String subString, final String type) throws Exception {

        if (ExpressionType.TAG.equals(type) || type == null) {
            // 构建订阅数据
            return buildSubscriptionData(topic, subString);
        }

        if (StringUtils.isEmpty(subString)) {
            throw new IllegalArgumentException("Expression can't be null! " + type);
        }

        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);

        return subscriptionData;
    }
}
