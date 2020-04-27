package com.flink.etl;

import com.alibaba.fastjson.JSON;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;



public class DataGenerator {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.3.122:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        List<String> events = Arrays.asList("page_view", "adv_click", "thumbs_up");
        Random random = new Random();
        Data data = null;
        ProducerRecord<String, String> record = null;
        try {
            while (true) {
                long timestamp = System.currentTimeMillis();
                String event = events.get(random.nextInt(events.size()));
                String uuid = UUID.randomUUID().toString();
                data = new Data(timestamp, event, uuid);
                record = new ProducerRecord<>("data-collection-topic", JSON.toJSONString(data));
                producer.send(record);
                Thread.sleep(3000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }

}
