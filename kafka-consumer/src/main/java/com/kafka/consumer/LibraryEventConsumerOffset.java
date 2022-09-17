package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import lombok.extern.slf4j.Slf4j;

   //@Component
@Slf4j
public class LibraryEventConsumerOffset implements AcknowledgingMessageListener<Integer, String> {

	@Override
	@KafkaListener(topics = "library-events")
	public void onMessage(ConsumerRecord<Integer, String> data, Acknowledgment acknowledgment) {
		// TODO Auto-generated method stub
		log.info("ConsumerRecord: {}", data);
		acknowledgment.acknowledge();
	}

}
