package com.kafka.producer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {
	
	@Autowired
	KafkaTemplate<Integer,String> kafkaTemplate;
	
	@Autowired
	ObjectMapper objectMapper;
	
	String topic = "library-events";
	
	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
		
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>> () {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				// TODO Auto-generated method stub
				
				handleSuccess(key, value,result);
				
			}


			@Override
			public void onFailure(Throwable ex) {
				// TODO Auto-generated method stub
				handleFailure(key, value,ex);
			}
			
		});
		
		
	}
	
	public ListenableFuture<SendResult<Integer, String>> sendLibraryEvent_Solution_2(LibraryEvent libraryEvent) throws JsonProcessingException {
		
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		ProducerRecord<Integer, String> producerRecord = buildProduerRecord(key,value, topic);
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>> () {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				// TODO Auto-generated method stub
				handleSuccess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {
				// TODO Auto-generated method stub
				handleFailure(key, value, ex);
				
			} 
			
		});
		
		return listenableFuture;
		
	}
	
	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws Exception {
		
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		SendResult<Integer, String> sendResult = null;
		
		try {
			sendResult = kafkaTemplate.sendDefault(key, value).get(1,TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			// TODO Auto-generated catch block
			log.error("Failure in sending message : {}", e.getMessage());
			throw e;
		} catch(Exception e) {
			log.error("Exception in Sending message : {}", e.getMessage());
			throw e;
		}
		
		return sendResult;
		
		
	}

	private ProducerRecord<Integer, String> buildProduerRecord(Integer key, String value, String topic2) {
		// TODO Auto-generated method stub
		List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
		return new ProducerRecord<>(topic, null,key, value, recordHeaders);
	}

	protected void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		// TODO Auto-generated method stub
		
		log.info("Message sent successfully for key : {} and value is {}, partition is {}", key, value, result.getRecordMetadata().partition());
		
	}

	protected void handleFailure(Integer key, String value, Throwable ex) {
		// TODO Auto-generated method stub
		
		try {
			throw ex;
			
		}catch( Throwable throwable) {
			
			log.error("error in onFailure: {}", throwable.getMessage());
		}
		
	}


	
	

}
