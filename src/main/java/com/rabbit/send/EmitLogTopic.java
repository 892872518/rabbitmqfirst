package com.rabbit.send;

import java.util.Random;
import java.util.UUID;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLogTopic {
    //转发器名称
    private  final  static  String EXCHANGE_NAME = "topic_logs";
    public static void main(String[] args) throws Exception{
        //创建连接到RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        //设置RabbitMQ所在主机ip或主机名
        factory.setHost("localhost");
        //创建一个连接
        Connection connection = factory.newConnection();
        //创建一个频道
        Channel channel = connection.createChannel();
        //声明转发器和类型
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        String[] routing_keys = new String[]{"kernal.info", "cron.warning","auth.info", "kernel.critical"};
        for (String routing_key:routing_keys) {
            String message = UUID.randomUUID().toString();
            // 发布消息至转发器，指定routingkey
            channel.basicPublish(EXCHANGE_NAME, routing_key, null, message.getBytes());
            System.out.println(" [x] Sent routingKey = "+routing_key+" ,message = " + message + ".");
        }
       //关闭频道和连接
       channel.close();
       connection.close();
    }
}
