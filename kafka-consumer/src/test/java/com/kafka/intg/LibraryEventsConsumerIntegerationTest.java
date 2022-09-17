package com.kafka.intg;

import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.LibraryEventConsumer;
import com.kafka.entity.Book;
import com.kafka.entity.LibraryEvent;
import com.kafka.entity.LibraryEventType;
import com.kafka.jpa.LibraryEventsRepository;
import com.kafka.service.LibraryEventService;


@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegerationTest {
	
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	KafkaListenerEndpointRegistry endpointRegistry;
	
	@SpyBean
	LibraryEventConsumer libraryEventConsumer;
	
	@SpyBean
	LibraryEventService libraryEventService;
	
	@Autowired
	LibraryEventsRepository libraryEventsReposity;
	
	@Autowired
	ObjectMapper objectMapper;
	
	private Consumer<Integer, String> consumer;
	
	@BeforeEach
	void setUp() {
		for(MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
		
		Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
		consumer = new DefaultKafkaConsumerFactory<>(configs,new IntegerDeserializer(),new StringDeserializer()).createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
		
	}
	
	@AfterEach
	void tearDown () {
		libraryEventsReposity.deleteAll();
		consumer.close();
	}
	
	@Test
	void publishNewLibraryEvent() throws InterruptedException, ExecutionException, JsonMappingException, JsonProcessingException {
		  //given
        String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();
        
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(6,TimeUnit.SECONDS);
        
        
      
        //then 
       // verify(libraryEventConsumer,times(1)).onMessage(isA(ConsumerRecord.class));
       // verify(libraryEventService,times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        
        //verify(libraryEventConsumer,times(1)).onMessage( consumerRecord);
        //verify(libraryEventService,times(1)).processLibraryEvent(consumerRecord);
        
        List<LibraryEvent> libraryEventList = (List<LibraryEvent>)libraryEventsReposity.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
        	assert libraryEvent.getLibraryEventId() != null;
        	assertEquals(456,libraryEvent.getBook().getBookId());
        	
        });
        
		
	}
	
	@Test
	void publishUpdateLibraryEvent() throws JsonMappingException, JsonProcessingException, InterruptedException, ExecutionException {
		  //given
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsReposity.save(libraryEvent);
        
        //publish the update LibraryEvent

        Book updatedBook = Book.builder().
                bookId(456).bookName("Kafka Using Spring Boot 2.x").bookAuthor("Dilip").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();
        
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        //verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        //verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsReposity.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka Using Spring Boot 2.x", persistedLibraryEvent.getBook().getBookName());
		
	}
	
	  @Test
	    void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
	        //given
	        Integer libraryEventId = 123;
	        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
	        System.out.println(json);
	        kafkaTemplate.sendDefault(libraryEventId, json).get();
	        //when
	        CountDownLatch latch = new CountDownLatch(1);
	        latch.await(3, TimeUnit.SECONDS);


	        //verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
	        //verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));

	        Optional<LibraryEvent> libraryEventOptional = libraryEventsReposity.findById(libraryEventId);
	        assertFalse(libraryEventOptional.isPresent());
	    }

	    @Test
	    void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
	        //given
	        Integer libraryEventId = null;
	        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
	        kafkaTemplate.sendDefault(libraryEventId, json).get();
	        //when
	        CountDownLatch latch = new CountDownLatch(1);
	        latch.await(3, TimeUnit.SECONDS);


	        //verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
	        //verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));
	    }

	
	
	
	
	

}
