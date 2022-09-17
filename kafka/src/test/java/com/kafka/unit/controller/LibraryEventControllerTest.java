package controller;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.KafkaApplication;
import com.kafka.controller.LibraryEventController;
import com.kafka.controller.LibraryEventControllerAdvice;
import com.kafka.entity.Book;
import com.kafka.entity.LibraryEvent;
import com.kafka.producer.LibraryEventProducer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@WebMvcTest(LibraryEventController.class)
@AutoConfigureMockMvc
@ContextConfiguration(classes = {LibraryEventController.class, LibraryEventProducer.class,LibraryEventControllerAdvice.class})
public class LibraryEventControllerTest {
	
	@Autowired
	MockMvc mockMvc;
	
	ObjectMapper objectMapper = new ObjectMapper();
	
	@MockBean
	LibraryEventProducer libraryEventProducer;
	
	
	@Test 
	void postLibraryEvent() throws Exception {
		Book book = Book.builder().bookId(123).bookName("kafka spring").bookAuthor("Alex W").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();
		String json = objectMapper.writeValueAsString(libraryEvent);
		
		when(libraryEventProducer.sendLibraryEvent_Solution_2(isA(LibraryEvent.class))).thenReturn(null);
		
		//expect 
		mockMvc.perform(post("/v1/libraryevent")
				.content(json)
				.contentType(MediaType.APPLICATION_JSON)).andExpect(status().isCreated());
		
	}
	
	
	@Test
	void postLibraryEvent_4XX() throws Exception {
		//given 
		  Book book = Book.builder()
	                .bookId(null)
	                .bookAuthor(null)
	                .bookName("Kafka using Spring Boot")
	                .build();

	        LibraryEvent libraryEvent = LibraryEvent.builder()
	                .libraryEventId(null)
	                .book(book)
	                .build();
	        
	        String json = objectMapper.writeValueAsString(libraryEvent);
	        
	        when(libraryEventProducer.sendLibraryEventSynchronous(isA(LibraryEvent.class))).thenReturn(null);
	        
	        //expect
	        String expectErrorMessage = "book.bookAuthor - must not be blank,book.bookId - must not be null";
	        
	        mockMvc.perform(post("/v1/libraryevent").content(json).contentType(MediaType.APPLICATION_JSON)).andExpect(status().is4xxClientError())
	        .andExpect(content().string(expectErrorMessage));
	        
	}
	
	
	@Test
	void updateLibraryEvent() throws Exception {
		
	      //given
        Book book = new Book().builder()
                .bookId(123)
                .bookAuthor("Dilip")
                .bookName("Kafka Using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(123)
                .book(book)
                .build();
        
        String json = objectMapper.writeValueAsString(libraryEvent);
        
        when(libraryEventProducer.sendLibraryEvent_Solution_2(isA(LibraryEvent.class))).thenReturn(null);
        
        //expect
        mockMvc.perform(put("/v1/libraryevent").content(json).contentType(MediaType.APPLICATION_JSON)).andExpect(status().isOk());
        
	}
	
	
	@Test
	void updateLibraryEventWithoutLibraryEventId() throws Exception {
		  //given
        Book book = new Book().builder()
                .bookId(123)
                .bookAuthor("Dilip")
                .bookName("Kafka Using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        String json = objectMapper.writeValueAsString(libraryEvent);
        
        when(libraryEventProducer.sendLibraryEvent_Solution_2(isA(LibraryEvent.class))).thenReturn(null);
        
        //except 
        
        mockMvc.perform(put("/v1/libraryevent").content(json).contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is4xxClientError()).andExpect(content().string("Please pass the library event ID"));
        
		
	}

}
