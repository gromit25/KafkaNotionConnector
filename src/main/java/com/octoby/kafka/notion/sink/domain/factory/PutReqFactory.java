package com.octoby.kafka.notion.sink.domain.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.RowReqDTO;
import com.octoby.kafka.notion.sink.domain.PutReqDTO;

/**
 * PutDTO 생성용 팩토리 클래스
 * 
 * @author jmsohn
 */
public class PutReqFactory extends RowReqFactory {

	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	protected PutReqFactory(Map<String, Object> jsonMap) {
		super(jsonMap);
	}

	@Override
	public RowReqDTO createDTO() {
		
		PutReqDTO dto = new PutReqDTO();
		
		dto.setKey(this.getKey());
		dto.setDataMap(this.getData());
		
		return dto;
	}
}
