package com.wangyichao.bigdata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AccessLogProducer {

    public static void main(String[] args) {
        // 实例化出producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(AccessLogProducer.getKafkaPropertis());

        List<String> accessLog = new ArrayList<String>();

//        accessLog.add("1,baidu.com,1000");
//        accessLog.add("2,baidu.com,2000");
//        accessLog.add("3,baidu,3000");
//        accessLog.add("1,jd.com,1000");
//        accessLog.add("2,jd.com,2000");
//        accessLog.add("1,taobao.com,1000");
        accessLog.add("4,taobao.com,1000");


        try {
            for (String value : accessLog) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("ACCESS_LOG", value);
                // 发送消息
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }


    public static Properties getKafkaPropertis() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return properties;
    }

}