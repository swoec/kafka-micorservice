package com.kafka.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.kafka.service.FailureService;
import com.kafka.service.LibraryEventService;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventConsumerConfig {
	
	public static final String RETRY = "RETRY";
	public static final String DEAD = "DEAD";
	public static final String SUCCESS = "SUCCESS";

	@Autowired
	LibraryEventService libraryEventService;

	@Autowired
	KafkaProperties kafkaProperties;
	
	@Autowired
	FailureService failureService;
	
	@Autowired
	KafkaTemplate kafkaTemplate;
	
	@Value("${topics.retry}")
	private String retryTopic;
	
	@Value("${topics.dlt}")
	private String deadLetterTopic;
	
    public DeadLetterPublishingRecoverer publishingRecoverer(){

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
           (r, e) -> {
        	   log.error("Exception in publish coverer: {}",e.getMessage(),e);
        	   if(e.getCause() instanceof RecoverableDataAccessException) {
        		   return new TopicPartition(retryTopic, r.partition());
        	   } else {
        		   return new TopicPartition(deadLetterTopic,r.partition());
        	   }
           });

        return recoverer;

    }
    
    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord,e)-> {
    	log.error("Exception in counsumerRecordRecoverer: {}", e.getMessage(),e);
    	var record = (ConsumerRecord<Integer, String>)consumerRecord;
    	if(e.getCause() instanceof RecoverableDataAccessException) {
    		log.info("inside recovery");
    		failureService.saveFailedRecord(record, e, RETRY);
    	} else {
    		log.info("inside non-recovery");
    		failureService.saveFailedRecord(record, e, DEAD);
    	}
    	
    };
	

	@Bean
	ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
		
		// factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
		
		ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory
				.getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.kafkaProperties.buildAdminProperties())));
		factory.setConcurrency(3);
		factory.setCommonErrorHandler(errorHandler());
//		factory.setErrorHandler((thrownException, data) -> {
//			log.info("Exception in consumer config is {} and the record is {}", thrownException.getMessage(), data);
//		});
//		factory.setRetryTemplate(retryTemplate());
		 
		

//		factory.setRecoveryCallback((context -> {
//			if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
//				log.info("Inside the recoverable logic");
//				Arrays.asList(context.attributeNames()).forEach(attributeName -> {
//					log.info("Attribute name is {}", attributeName);
//					log.info("attribute value is {}", context.getAttribute(attributeName));
//				});
//				ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context
//						.getAttribute("record");
//				libraryEventService.handleRecovery(consumerRecord);
//
//			} else {
//				log.info("Inside the non recoverable logic");
//				throw new RuntimeException(context.getLastThrowable().getMessage());
//			}
//
//			return null;
//		}));

		return factory;

	}

	private DefaultErrorHandler errorHandler() {
		// TODO Auto-generated method stub
		var exceptionToIgnoreList = List.of(IllegalArgumentException.class);
		
		var exceptionToRetryList = List.of(RecoverableDataAccessException.class);
		
		var fixedBackOff = new FixedBackOff(1000L,2);
		var expBackOff = new ExponentialBackOffWithMaxRetries(2);
		expBackOff.setInitialInterval(1000L);
		expBackOff.setMultiplier(2.0);
		expBackOff.setMaxInterval(2000L);
		
		var errorHandler = new DefaultErrorHandler(consumerRecordRecoverer,expBackOff);
		
		exceptionToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
		//exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions);
		errorHandler.setRetryListeners(((record,ex,deliveryAttempt)-> {
			log.info("Failed Record in Retry Listener , Exception: {},devliveryAttempt: {}",ex.getMessage(),deliveryAttempt);
		}));
		
		return errorHandler;
	}

	private RetryTemplate retryTemplate() {
		// TODO Auto-generated method stub
		FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
		fixedBackOffPolicy.setBackOffPeriod(1000);
		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(simpleRetryPolicy());
		retryTemplate.setBackOffPolicy(fixedBackOffPolicy);

		return retryTemplate;
	}

	private RetryPolicy simpleRetryPolicy() {
		// TODO Auto-generated method stub
		Map<Class<? extends Throwable>, Boolean> exceptionsMap = new HashMap<>();
		exceptionsMap.put(IllegalArgumentException.class, false);
		exceptionsMap.put(RecoverableDataAccessException.class, true);
		SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionsMap, true);

		return simpleRetryPolicy;
	}

}
