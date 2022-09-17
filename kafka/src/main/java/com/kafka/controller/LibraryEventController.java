package com.kafka.controller;

import java.util.concurrent.ExecutionException;

import javax.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.entity.LibraryEvent;
import com.kafka.entity.LibraryEventType;
import com.kafka.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LibraryEventController {
	
	//final Logger logger = LoggerFactory.getLogger(LibraryEventController.class);
	
	@Autowired
	LibraryEventProducer libraryEventProducer;

	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent>PostLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, MethodArgumentNotValidException {
		//logger.info("libraryEvent: {}", libraryEvent);
		log.info("libraryEvent: {}", libraryEvent);
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		//libraryEventProducer.sendLibraryEvent(libraryEvent);
		try {
			libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
		} catch (MethodArgumentNotValidException e) {
			// TODO Auto-generated catch block
			throw e;
		} catch (Exception e) {
			// TODO Auto-generated catch block
		log.error("error message: {}", e.getMessage());
		}
		//libraryEventProducer.sendLibraryEvent_Solution_2(libraryEvent);
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
		
	}
	
	@PutMapping("/v1/libraryevent")
	public ResponseEntity<?>modifyLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws Exception {
		
		if(libraryEvent.getLibraryEventId() == null ) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the library event ID");
		}
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		//libraryEventProducer.sendLibraryEvent(libraryEvent);
	    libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
	    //libraryEventProducer.sendLibraryEvent_Solution_2(libraryEvent);
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
		
	}

}
