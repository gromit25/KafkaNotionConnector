package com.octoby.kafka.notion.sink.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.ReqDTO;
import com.octoby.kafka.notion.sink.process.PostReqProcess;
import com.octoby.kafka.notion.sink.process.ReqProcess;
import com.octoby.kafka.notion.sink.domain.PostReqDTO;

/**
 * PostDTO 생성용 팩토리 클래스
 * 
 * @author jmsohn
 */
public class PostReqFactory extends ReqFactory {

	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	PostReqFactory(Map<String, Object> jsonMap) {
		super(jsonMap);
	}

	@Override
	public ReqDTO createConcreteDTO() {
		
		PostReqDTO dto = new PostReqDTO();
		
		dto.setDataMap(this.getData());
		
		return dto;
	}

	@Override
	public ReqProcess createProcess() {
		return new PostReqProcess();
	}
}
