package com.octoby.kafka.notion.sink.domain.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.RowReqDTO;
import com.octoby.kafka.notion.util.JSONUtil;

/**
 * 메시지 DTO 객체 생성 팩토리 추상 클래스 
 * 
 * @author jmsohn
 */
public abstract class RowReqFactory {
	
	/**
	 * 팩토리 객체 생성<br>
	 * 메시지의 'method' 항목에 따라 생성
	 * 
	 * @param jsonStr 메시지
	 * @return 메시지 팩토리
	 */
	public static RowReqFactory create(String jsonStr) throws Exception {
		
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
	public abstract RowReqDTO genDTO();
	
	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	protected RowReqFactory(Map<String, Object> jsonMap) {
		this.jsonMap = jsonMap;
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
}
