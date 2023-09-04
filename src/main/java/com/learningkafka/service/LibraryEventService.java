package com.learningkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learningkafka.entity.LibraryEvent;
import com.learningkafka.repository.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.io.DataInput;
import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);

        if (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 000) {
            throw new RecoverableDataAccessException("Temporary network issue");
        }
        log.info("Processing the record");
        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validation(libraryEvent);
                save(libraryEvent);
                break;
        }
    }

    private void validation(LibraryEvent libraryEvent) {

        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library eventId is missing");
        }
        Optional<LibraryEvent> byId = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
        if (!byId.isPresent()) {
            throw new IllegalArgumentException("Not a valid Library event");
        }
        log.info("Successfully validated libraryEvent with id :{}", libraryEvent.getLibraryEventId());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
        log.info("SuccessFully persisted libraryEvent: {}", libraryEvent);
    }

    public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord) {
        Integer key = consumerRecord.key();
        String message = consumerRecord.value();

        ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture = kafkaTemplate.sendDefault(key, message);
        sendResultListenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, message, result);

            }

            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, message, ex);

            }
        });

    }

    protected void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error in sending the message to broker with exception is {}", ex.getMessage());

        try {
            throw ex;
        } catch (Throwable e) {
            log.error("Error in failure: {}", e.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Succsessfully send the message to broker with key: {}, and values is :{}, partition is :{}", key,
                value, result.getRecordMetadata().partition());

    }
}
