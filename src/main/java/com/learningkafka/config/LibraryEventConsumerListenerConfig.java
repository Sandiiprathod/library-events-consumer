package com.learningkafka.config;

import com.learningkafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


@Configuration
@EnableKafka
@Slf4j
public class LibraryEventConsumerListenerConfig {

    @Autowired
    LibraryEventService libraryEventService;
    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        //Custom error handling
        factory.setErrorHandler((throException,data)->{
            log.info("Exception in ConsumerConfig is {} and with data is {}",throException.getMessage(),data);
            //persist error for monitoring data
        });
        factory.setRetryTemplate(simpleRetry());
        //recovery in case retry exausted
        factory.setRecoveryCallback(context -> {
            if(context.getLastThrowable().getCause() instanceof  RecoverableDataAccessException){
                log.info("Inside recoverable logic");
                    /*Arrays.asList(context.attributeNames()).forEach(attributeName->{
                        log.info("Attribute name is :{}",attributeName);
                        log.info("Attribute value is : {}",context.getAttribute(attributeName));
                    });*/
                ConsumerRecord<Integer,String> record = (ConsumerRecord<Integer, String>) context.getAttribute("record");
                libraryEventService.handleRecovery(record);

            }else{
                log.info("inside non recoverable logic",context.getLastThrowable().getMessage());
            }
            return null;
        });
        return factory;
    }

    //Retry operations when any record consumption got failed
    private RetryTemplate simpleRetry() {
        FixedBackOffPolicy fixedBackOffPolicy=new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);
        RetryTemplate retryTemplate =new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    private RetryPolicy retryPolicy() {

        /*SimpleRetryPolicy simpleRetryPolicy =new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(3);*/
        //retry policy based on exception occured
        Map<Class<? extends Throwable>,Boolean> exceptionMap=new HashMap<>();
        exceptionMap.put(IllegalArgumentException.class, false);// doesn't retry
        exceptionMap.put(RecoverableDataAccessException.class, true);// retry 3 times
        SimpleRetryPolicy simpleRetryPolicy= new SimpleRetryPolicy(3,exceptionMap  ,true );
        return simpleRetryPolicy;
    }


}

