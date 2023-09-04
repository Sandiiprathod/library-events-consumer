/*
package com.learningkafka.consumer;

import ch.qos.logback.core.net.SyslogOutputStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learningkafka.entity.Book;
import com.learningkafka.entity.LibraryEvent;
import com.learningkafka.entity.LibraryEventType;
import com.learningkafka.repository.LibraryEventRepository;
import com.learningkafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = "library-event",partitions = 3)
@TestPropertySource(properties = {"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}","spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    @Spy
    LibraryEventConsumer libraryEventConsumerSpy;

    @Spy
    LibraryEventService libraryEventServiceSpy;

    @Autowired
    ObjectMapper objectMapper;
    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListener : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListener, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @Test
    public void publishLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        //when
        String json="{\\\"libraryEventId\\\":null,\\\"libraryEventType\\\":\\\"NEW\\\",\\\"book\\\":{\\\"bookId\\\":456,\\\"bookName\\\":\\\"Kafka Using Spring Boot\\\",\\\"bookAuthor\\\":\\\"Dilip\\\"}}";

        kafkaTemplate.sendDefault(json).get();
        //given
        CountDownLatch latch=new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);
        //then
        verify(libraryEventConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEVentList = (List<LibraryEvent>) libraryEventRepository.findAll();
        assert libraryEVentList.size()==1;
        libraryEVentList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() !=null;
            assertEquals(456,libraryEvent.getBook().getBookId());
        });
    }

    @Test
    public void updateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        //given
        //save the library event first
        String json="{\\\"libraryEventId\\\":null,\\\"libraryEventType\\\":\\\"NEW\\\",\\\"book\\\":{\\\"bookId\\\":456,\\\"bookName\\\":\\\"Kafka Using Spring Boot\\\",\\\"bookAuthor\\\":\\\"Dilip\\\"}}";
        LibraryEvent libraryEvent=objectMapper.readValue(json, LibraryEvent.class);
        System.out.println(libraryEvent);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);

        Book updatedBook=Book.builder().bookId(456).bookName("Kafka Using Spring Boot 2.x").author("Dilip").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson=objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),updatedJson).get();
        //when
        CountDownLatch latch=new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);
        //then
        LibraryEvent persistedLibraryEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka Using Spring Boot 2.x", persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    public void publishLibraryEvent_Null_Event_Id() throws ExecutionException, InterruptedException, JsonProcessingException {
        //when
        String libraryEventId=null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";

        kafkaTemplate.sendDefault(json).get();
        //given
        CountDownLatch latch=new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);
        //then
        verify(libraryEventConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));

    }
}
*/
