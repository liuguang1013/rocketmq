package org.apache.rocketmq.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @className: Test_Consumer
 * @description:
 * @author: whr
 * @date: 2025/4/7 16:30
 * @version: 1.0.0
 **/
public class Test_Consumer {

    public static void main(String[] args) {

        try {
            DefaultMQPushConsumer consumer1 = new DefaultMQPushConsumer("normalConsumerGroup1");
            consumer1.subscribe("topic1", "*");
            consumer1.setNamesrvAddr("127.0.0.1:9876");
            consumer1.setMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    if (msgs.size() >= 1) {
                        MessageExt messageExt = msgs.get(0);
                        System.out.println("consumer1 接收消息: " + messageExt.getTopic() + "-" +
                                messageExt.getQueueId() + "-" +
                                new String(messageExt.getBody(), StandardCharsets.UTF_8));
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer1.start();


            DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer("normalConsumerGroup2");
            consumer2.subscribe("topic1", "tag-2");
            consumer2.setNamesrvAddr("127.0.0.1:9876");
            consumer2.setMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    if (msgs.size() >= 1) {
                        MessageExt messageExt = msgs.get(0);
                        System.out.println("consumer2 接收消息: " + messageExt.getTopic() + "-" +
                                messageExt.getQueueId() + "-" +
                                new String(messageExt.getBody(), StandardCharsets.UTF_8));
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer2.start();
            System.out.println("消费者启动成功 ---------------");
        } catch (MQClientException e) {
            e.printStackTrace();
        }

    }
}
