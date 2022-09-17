package com.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.kafka.entity.FailRecord;
import com.kafka.jpa.FailureRecordRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class FailureService {
	
	@Autowired
	private FailureRecordRepository failureRecordRepository;
	
	public void saveFailedRecord(ConsumerRecord<Integer, String> consumerRecord,Exception e, String status) {
		
		var failRecord = new FailRecord(null, consumerRecord.topic(),consumerRecord.key(),consumerRecord.value(),consumerRecord.partition()
				,consumerRecord.offset(),e.getCause().getMessage(),status);
		failureRecordRepository.save(failRecord);
	}
}
