package com.octoby.kafka.notion.sink.domain.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.MessageDTO;
import com.octoby.kafka.notion.sink.domain.PostDTO;

/**
 * 
 * 
 * @author jmsohn
 */
public class PostFactory extends MessageFactory {

	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	PostFactory(Map<String, Object> jsonMap) {
		super(jsonMap);
	}

	@Override
	public MessageDTO genDTO() {
		
		PostDTO dto = new PostDTO();
		
		dto.setDataMap(this.getData());
		
		return dto;
	}
}
