package com.octoby.kafka.notion.sink.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.RowReqDTO;
import com.octoby.kafka.notion.sink.process.ReqProcess;
import com.octoby.kafka.notion.util.JSONUtil;

/**
 * 메시지 DTO 객체 생성 팩토리 추상 클래스 
 * 
 * @author jmsohn
 */
public abstract class ReqFactory {
	
	/**
	 * 팩토리 객체 생성<br>
	 * 메시지의 'method' 항목에 따라 생성
	 * 
	 * @param jsonStr 메시지
	 * @return 메시지 팩토리
	 */
	public static ReqFactory create(String jsonStr) throws Exception {
		
		Map<String, Object> jsonMap = JSONUtil.parseMap(jsonStr);
		
		String method = jsonMap.get("method").toString();
		
		return switch(method) {
			case "PUT" -> new PutReqFactory(jsonMap);
			case "POST" -> new PostReqFactory(jsonMap);
			case "DELETE" -> new DeleteReqFactory(jsonMap);
			default -> throw new IllegalArgumentException("invalid method type: " + method);
		};
	}
	
	
	// -----------------------------------------------------
	
	
	/** 파싱된 메시지 객체 */
	private Map<String, Object> jsonMap;
	
	
	/**
	 * DTO 생성 추상 메소드
	 * 
	 * @return 생성된 DTO 객체
	 */
	protected abstract RowReqDTO createConcreteDTO();
	
	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	protected ReqFactory(Map<String, Object> jsonMap) {
		this.jsonMap = jsonMap;
	}
	
	/**
	 * DTO 생성 추상 메소드
	 * 
	 * @return 생성된 DTO 객체
	 */
	public RowReqDTO createDTO() {
		
		RowReqDTO dto = this.createConcreteDTO();
		
		dto.setDbId(this.getDbId());
		
		return dto;
	}
	
	/**
	 * 노션 DB 아이디 반환
	 * 
	 * @return 노션 DB 아이디
	 */
	private String getDbId() {
		
		if(this.jsonMap.containsKey("dbid") == false) {
			throw new IllegalArgumentException("'dbid' attribute is not found.");
		}
		
		return this.jsonMap.get("dbid").toString();
	}
	
	/**
	 * 메시지에서 키값 추출 후 반환
	 * 
	 * @return 키값
	 */
	protected String getKey() {
		
		if(this.jsonMap.containsKey("key") == false) {
			throw new IllegalArgumentException("'key' attribute is not found.");
		}
		
		return this.jsonMap.get("key").toString();
	}
	
	/**
	 * 메시지에서 데이터맵 추출 후 반환
	 * 
	 * @return 데이터맵
	 */
	@SuppressWarnings("unchecked")
	protected Map<String, Object> getData() {
		
		if(this.jsonMap.containsKey("data") == false) {
			throw new IllegalArgumentException("'data' attribute is not found.");
		}
		
		return (Map<String, Object>)this.jsonMap.get("data");
	}
	
	// ------------------------------
	
	/**
	 * 처리 프로세스 객체 생성 및 반환
	 * 
	 * @return 처리 프로세스 객체
	 */
	public abstract ReqProcess createProcess();
}
