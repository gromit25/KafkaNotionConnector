package com.octoby.kafka.notion.sink.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.ReqDTO;
import com.octoby.kafka.notion.sink.process.PutReqProcess;
import com.octoby.kafka.notion.sink.process.ReqProcess;
import com.octoby.kafka.notion.sink.domain.PutReqDTO;

/**
 * PutDTO 생성용 팩토리 클래스
 * 
 * @author jmsohn
 */
public class PutReqFactory extends ReqFactory {

	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	protected PutReqFactory(Map<String, Object> jsonMap) {
		super(jsonMap);
	}

	@Override
	public ReqDTO createConcreteDTO() {
		
		PutReqDTO dto = new PutReqDTO();
		
		dto.setKey(this.getKey());
		dto.setDataMap(this.getData());
		
		return dto;
	}

	@Override
	public ReqProcess createProcess() {
		return new PutReqProcess();
	}
}
