package com.octoby.kafka.notion.sink.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.RowReqDTO;
import com.octoby.kafka.notion.sink.domain.PostReqDTO;

/**
 * PostDTO 생성용 팩토리 클래스
 * 
 * @author jmsohn
 */
public class PostReqFactory extends RowReqFactory {

	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	PostReqFactory(Map<String, Object> jsonMap) {
		super(jsonMap);
	}

	@Override
	public RowReqDTO createDTO() {
		
		PostReqDTO dto = new PostReqDTO();
		
		dto.setDataMap(this.getData());
		
		return dto;
	}
}
