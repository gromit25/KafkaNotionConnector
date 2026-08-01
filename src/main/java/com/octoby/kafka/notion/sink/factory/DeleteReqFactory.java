package com.octoby.kafka.notion.sink.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.DeleteReqDTO;
import com.octoby.kafka.notion.sink.domain.RowReqDTO;

/**
 * DeleteDTO 생성용 팩토리 클래스
 * 
 * @author jmsohn
 */
public class DeleteReqFactory extends RowReqFactory {
	
	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	DeleteReqFactory(Map<String, Object> jsonMap) {
		super(jsonMap);
	}

	@Override
	public RowReqDTO createDTO() {
		
		DeleteReqDTO dto = new DeleteReqDTO();
		
		dto.setKey(this.getKey());
		
		return dto;
	}
}
