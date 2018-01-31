package com.rabbit.receive;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class ReceiveLogsTopic1 {
    //转发器名称
    private  final  static  String EXCHANGE_NAME = "topic_logs";
    public static void main(String[] args) throws Exception {
        //区分不同工作进程的输出
        int hashCode = ReceiveLogsTopic1.class.hashCode();
        //创建连接到RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        //设置RabbitMQ所在主机ip或主机名
        factory.setHost("localhost");
        //创建一个连接
        Connection connection = factory.newConnection();
        //创建一个频道
        Channel channel = connection.createChannel();
        //声明转发器和类型
        channel.exchangeDeclare(EXCHANGE_NAME,"topic");
        // 创建一个非持久的、唯一的且自动删除的队列
        String queueName = channel.queueDeclare().getQueue();
        //接收所有与kernel相关的消息
        channel.queueBind(queueName,EXCHANGE_NAME,"kernel.*");
        System.out.println(hashCode+"-->准备发送消息...");
        //创建队列消费者
        QueueingConsumer consumer = new QueueingConsumer(channel);
        // 指定接收者，第二个参数为自动应答，无需手动应答
        channel.basicConsume(queueName,true,consumer);
        while(true){
            //nextDelivery是一个阻塞方法（内部实现其实是阻塞队列的take方法）
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            String routingKey = delivery.getEnvelope().getRoutingKey();
            System.out.println(hashCode+"收到routingKey：" + routingKey + ",msg = " + message);
        }
    }
}
