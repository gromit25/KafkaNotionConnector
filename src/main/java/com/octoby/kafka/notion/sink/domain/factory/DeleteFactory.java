package com.octoby.kafka.notion.sink.domain.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.DeleteDTO;
import com.octoby.kafka.notion.sink.domain.MessageDTO;

/**
 * 
 * 
 * @author jmsohn
 */
public class DeleteFactory extends MessageFactory {
	
	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	DeleteFactory(Map<String, Object> jsonMap) {
		super(jsonMap);
	}

	@Override
	public MessageDTO genDTO() {
		
		DeleteDTO dto = new DeleteDTO();
		
		dto.setKey(this.getKey());
		
		return dto;
	}
}
