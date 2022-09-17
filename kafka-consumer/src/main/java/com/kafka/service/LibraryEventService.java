package com.kafka.service;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.LibraryEvent;
import com.kafka.jpa.LibraryEventsRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventService {

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	private LibraryEventsRepository libraryEventRepository;

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord)
			throws JsonMappingException, JsonProcessingException {

		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info(" process library event: {}", libraryEvent);

		switch (libraryEvent.getLibraryEventType()) {
		case NEW:
			save(libraryEvent);
			break;
		case UPDATE:
			validate(libraryEvent);
			save(libraryEvent);
			break;
		default:
			log.info(" wrong action!");
		}
	}

	private void validate(LibraryEvent libraryEvent) {
		// TODO Auto-generated method stub
		if (libraryEvent.getLibraryEventId() == null) {
			throw new IllegalArgumentException("library event id is missing");
		}
		Optional<LibraryEvent> libraryEventOptional = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
		if (!libraryEventOptional.isPresent()) {
			throw new IllegalArgumentException("no valid library event to update");
		}
		log.info(" Validatation is successfully completed: {}", libraryEventOptional.get());

	}

	private void save(LibraryEvent libraryEvent) {
		// TODO Auto-generated method stub
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventRepository.save(libraryEvent);
		log.info("Library event saved: {}", libraryEvent);

	}

	public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord) {
		// TODO Auto-generated method stub
		Integer key = consumerRecord.key();
		String message = consumerRecord.value();

		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, message);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				// TODO Auto-generated method stub
				handleSuccess(key, message, result);

			}

			@Override
			public void onFailure(Throwable ex) {
				// TODO Auto-generated method stub

				handleFailure(key, message, ex);

			}
		});
	}

	protected void handleSuccess(Integer key, String message, SendResult<Integer, String> result) {
		// TODO Auto-generated method stub
		log.info("Message sent successfully for the key: {}, and value is {}, partition is {}", key, message,
				result.getRecordMetadata().partition());

	}

	protected void handleFailure(Integer key, String message, Throwable ex) {
		// TODO Auto-generated method stub
		try {
			throw ex;
		} catch (Throwable throwable) {
			log.error("Error sending message and the exception is {}", throwable.getMessage());
		}

	}

}
