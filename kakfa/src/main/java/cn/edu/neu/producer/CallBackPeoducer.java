package cn.edu.neu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CallBackPeoducer {
    public static void main(String[] args) {
        Properties pros = new Properties();
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("second", 0,"neu","hello--" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null){
                        System.out.println(metadata.partition() + "--"+ metadata.offset());
                    }
                }
            });

        }
        producer.close();
    }
}
