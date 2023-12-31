package com.learningkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

//@Component
@Slf4j
public class LibraryEventConsumerManualOffset  implements AcknowledgingMessageListener<Integer,String> {

    @Override
    public void onMessage(ConsumerRecord data, Acknowledgment acknowledgment) {
        log.info("ConsumerRecords :{}",data);
        acknowledgment.acknowledge();
    }
}
