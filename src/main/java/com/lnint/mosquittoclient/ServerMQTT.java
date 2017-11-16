package com.lnint.mosquittoclient;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * 消息发布者：服务器向多个客户端推送主题，即不同客户端可向服务器订阅相同主题
 * mosquitto安装步骤请参考http://blog.csdn.net/wangpf2011/article/details/78542018
 * 
 * @author wangpf 2017年10月1日
 */
public class ServerMQTT {

    // tcp://MQTT安装的服务器地址:MQTT定义的端口号
    public static final String HOST = "tcp://192.168.8.118:1883";
    // 定义一个主题
    public static final String TOPIC = "root/topic/msg";
    // 定义MQTT的ID，可以在MQTT服务配置中指定
    private static final String clientid = "myserverid";

    private MqttClient client;
    private MqttTopic topic11;
    private String userName = "wangpf";
    private String passWord = "wangpf";
    
    private MqttMessage message;

    /**
     * 构造函数
     * 
     * @throws MqttException
     */
    public ServerMQTT() throws MqttException {
        // MemoryPersistence设置clientid的保存形式，默认为以内存保存
        client = new MqttClient(HOST, clientid, new MemoryPersistence());
        connect();
    }

    /**
     * 用来连接服务器
     */
    private void connect() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        options.setUserName(userName);
        options.setPassword(passWord.toCharArray());
        // 设置超时时间
        options.setConnectionTimeout(10);
        // 设置会话心跳时间
        options.setKeepAliveInterval(20);
        try {
            client.setCallback(new PushCallback("myserverid"));
            client.connect(options);

            topic11 = client.getTopic(TOPIC);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 发布消息
     * @param topic
     * @param message
     * @throws MqttPersistenceException
     * @throws MqttException
     */
    public void publish(MqttTopic topic, MqttMessage message) throws MqttPersistenceException, MqttException {
        MqttDeliveryToken token = topic.publish(message);
        token.waitForCompletion();
        System.out.println("message is published completely " + token.isComplete());
    }

    /**
     * 启动入口
     * 
     * @param args
     * @throws MqttException
     */
    public static void main(String[] args) throws MqttException {
        ServerMQTT server = new ServerMQTT();
        
        server.message = new MqttMessage();
        server.message.setQos(1);
        server.message.setRetained(true);
        server.message.setPayload("hello,哇哇。。。。".getBytes());
        server.publish(server.topic11, server.message);
        System.out.println("ratained status "+server.message.isRetained());
    }
}
