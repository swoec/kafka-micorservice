package com.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.kafka.entity.Book;
import com.kafka.entity.LibraryEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers = ${spring.embedded.kafka,brokers}",
		"spring.kafka.admin.properties.bootstrap.servers= ${spring.embedded.kafka.brokers}"})
class KafkaApplicationTests {

	@Autowired
	TestRestTemplate restTemplate;
	
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	
	private Consumer<Integer, String> consumer;
	
	@BeforeEach
	void setUp() {
		Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
		consumer = new DefaultKafkaConsumerFactory<>(configs,new IntegerDeserializer(),new StringDeserializer()).createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
		
	}
	
	@AfterEach
	void tearDown () {
		consumer.close();
	}
	
	@Test
	@Timeout(5)
	void postLibraryEvent() {
		
		Book book = Book.builder().bookId(123).bookName("kafka spring").bookAuthor("Alex W").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		HttpHeaders headers = new HttpHeaders();
		headers.set("content-type", MediaType.APPLICATION_JSON.toString());
		HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent,headers);
		//when
		ResponseEntity<LibraryEvent>responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST,request,LibraryEvent.class);
		
		//then
		assertEquals(HttpStatus.CREATED,responseEntity.getStatusCode());
		
		ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
		
		String expected = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"kafka spring\",\"bookAuthor\":\"Alex W\"}}";;
		String value = consumerRecord.value();
		assertEquals(expected, value);
		
		
	}
}
