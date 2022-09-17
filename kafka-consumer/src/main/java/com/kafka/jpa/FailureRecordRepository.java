package com.kafka.jpa;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

import com.kafka.entity.FailRecord;

public interface FailureRecordRepository extends CrudRepository <FailRecord, Integer>{
	 List<FailRecord> findAllByStatuss(String retry);

}
