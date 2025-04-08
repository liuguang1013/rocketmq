package org.apache.rocketmq.test;

import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

/**
 * @className: Test_Producer
 * @description:
 * @author: whr
 * @date: 2024/11/26 17:02
 * @version: 1.0.0
 **/
public class Test_Producer {

    public static void main(String[] args) {
//        BrokerStartup.main(null);

        try {

            DefaultMQProducer defaultMQProducer = new DefaultMQProducer("testProducer");
            defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
            defaultMQProducer.start();


            System.out.println("开始发送消息 ---------------");

//            testSendMsg(defaultMQProducer);

            String msg = "普通测试消息 "+ LocalDateTime.now();
            Message message = new Message("topic1", "tag-3", "", 0,
                    msg.getBytes(StandardCharsets.UTF_8), true);
            SendResult send = defaultMQProducer.send(message);
            System.out.println("send = " + send);


            Thread.sleep(300000000000L);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void testSendMsg(DefaultMQProducer defaultMQProducer) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        Message message = new Message();
        message.setDelayTimeLevel(0);
        message.setBody("message".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < 4; i++) {
            System.out.println("生产者发送消息：" + i);
            message.setTopic("topic1");
            defaultMQProducer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get((int) arg);
                }
            }, i);

            message.setTopic("example");
            defaultMQProducer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get((int) arg);
                }
            }, i);
        }
    }
}
