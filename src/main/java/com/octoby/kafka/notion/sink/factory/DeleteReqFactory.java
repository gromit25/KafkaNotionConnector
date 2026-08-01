package com.octoby.kafka.notion.sink.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.DeleteReqDTO;
import com.octoby.kafka.notion.sink.domain.ReqDTO;
import com.octoby.kafka.notion.sink.process.DeleteReqProcess;
import com.octoby.kafka.notion.sink.process.ReqProcess;

/**
 * DeleteDTO 생성용 팩토리 클래스
 * 
 * @author jmsohn
 */
public class DeleteReqFactory extends ReqFactory {
	
	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	DeleteReqFactory(Map<String, Object> jsonMap) {
		super(jsonMap);
	}

	@Override
	public ReqDTO createConcreteDTO() {
		
		DeleteReqDTO dto = new DeleteReqDTO();
		
		dto.setKey(this.getKey());
		
		return dto;
	}

	@Override
	public ReqProcess createProcess() {
		return new DeleteReqProcess();
	}
}
