package cn.edu.neu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String, String> {
    int sucess;
    int errors;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            sucess++;

        } else {
            errors++;
        }
    }
    @Override
    public void close() {
        System.out.println("success:" + sucess);
        System.out.println("errors:" + errors);
    }
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
