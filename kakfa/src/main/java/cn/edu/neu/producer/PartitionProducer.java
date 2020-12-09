package cn.edu.neu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class PartitionProducer {
    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        pros.put("partition.class", "cn.edu.neu.partitioner.MyPartitioner");

        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("second", "neu","hello--" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null){
                        System.out.println(metadata.partition() + "--"+ metadata.offset());
                    }
                }
            });

        }

//        System.out.println("closed");
        producer.close();
    }
}
