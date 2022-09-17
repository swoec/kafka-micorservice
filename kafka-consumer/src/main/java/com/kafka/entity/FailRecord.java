package com.kafka.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class FailRecord {
	
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Integer FailId;
	
	
	private String topics;
	private Integer keys;
	private String errorRecord;
	private Integer partition;
	private Long offset_values;
	private String exceptions;
	private String statuss;

}
