package com.octoby.kafka.notion.sink.domain.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.MessageDTO;
import com.octoby.kafka.notion.sink.domain.PutDTO;

/**
 * 
 * 
 * @author jmsohn
 */
public class PutFactory extends MessageFactory {

	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	protected PutFactory(Map<String, Object> jsonMap) {
		super(jsonMap);
	}

	@Override
	public MessageDTO genDTO() {
		
		PutDTO dto = new PutDTO();
		
		dto.setKey(this.getKey());
		dto.setDataMap(this.getData());
		
		return dto;
	}
}
