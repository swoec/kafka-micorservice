package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.kafka.service.LibraryEventService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventRetryConsumer {
	
	@Autowired
	private LibraryEventService libraryEventService;
	
	@KafkaListener(topics = {"${topics.retry}"},
			autoStartup = "${retryListener.startup:false}", 
			groupId = "retry-listen-group")
	
	 public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		log.info("ConsumerRecord in Retry Consumer: {}", consumerRecord);
		consumerRecord.headers().forEach(header -> {
			log.info("key: {}, value: {}", header.key(),new String(header.value()));
		});
		
		libraryEventService.processLibraryEvent(consumerRecord);
		
	}

}
