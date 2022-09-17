package com.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.config.LibraryEventConsumerConfig;
import com.kafka.entity.FailRecord;
import com.kafka.jpa.FailureRecordRepository;
import com.kafka.service.LibraryEventService;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

//@Component
@Slf4j
public class RetryScheduler {

    private FailureRecordRepository failureRecordRepository;
    private LibraryEventService libraryEventsService;

    public RetryScheduler(FailureRecordRepository failureRecordRepository, LibraryEventService libraryEventsService) {
        this.failureRecordRepository = failureRecordRepository;
        this.libraryEventsService = libraryEventsService;
    }

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords(){

        log.info("Retying Failed Records Started!");
        failureRecordRepository.findAllByStatuss(LibraryEventConsumerConfig.RETRY)
                .forEach(failureRecord -> {
                    log.info("Retying Failed Record : {} ", failureRecord);
                    var consumerRecord = buildConsumerRecord(failureRecord);
                    try {
                        libraryEventsService.processLibraryEvent(consumerRecord);
                        failureRecord.setStatuss(LibraryEventConsumerConfig.SUCCESS);
                        failureRecordRepository.save(failureRecord);
                    } catch (Exception e) {
                        log.error("Exception in retryFailedRecords : {}", e.getMessage(), e);
                    }
                });
        log.info("Retying Failed Records Completed!");

    }

    private ConsumerRecord<Integer,String> buildConsumerRecord(FailRecord failureRecord) {

        return new ConsumerRecord<>(
                failureRecord.getTopics(),
                failureRecord.getPartition(),
                failureRecord.getOffset_values(),
                failureRecord.getKeys(),
                failureRecord.getErrorRecord()
        );

    }
}
