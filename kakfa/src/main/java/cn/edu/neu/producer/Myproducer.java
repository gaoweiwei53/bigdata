package cn.edu.neu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Myproducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("acks","all");
//        properties.put("retries",1);
//        // 16k
//        properties.put("batch.size",16384);
//        properties.put("linger.ms",1);
//        // RecordAccumulator buffer  32m
//        properties.put("buffer.memory",3354432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(properties);
        //System.out.println("closed");

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first", "nihao","hello"+ i));
            System.out.println(i);
        }
//        System.out.println("closed");
        producer.close();
    }
}
