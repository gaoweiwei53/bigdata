package cn.edu.neu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        pros.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        pros.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        pros.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        pros.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // consumer group
        pros.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata");

        // reset consumer's offfset
        pros.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // must change group name

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(pros);

        // subscribe topic
        consumer.subscribe(Arrays.asList("first", "second"));

        // get data
        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "--" + consumerRecord.value());
            }
        }
    }
}
