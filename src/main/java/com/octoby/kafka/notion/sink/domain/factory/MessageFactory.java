package com.octoby.kafka.notion.sink.domain.factory;

import java.util.Map;

import com.octoby.kafka.notion.sink.domain.MessageDTO;
import com.octoby.kafka.notion.util.JSONUtil;

import lombok.Getter;

/**
 * 
 * 
 * @author jmsohn
 */
public abstract class MessageFactory {
	
	/**
	 * 
	 * 
	 * @param jsonStr
	 * @return
	 */
	public static MessageFactory create(String jsonStr) throws Exception {
		
		Map<String, Object> jsonMap = JSONUtil.parseMap(jsonStr);
		
		String method = jsonMap.get("method").toString();
		
		return switch(method) {
			case "PUT" -> new PutFactory(jsonMap);
			case "POST" -> new PostFactory(jsonMap);
			case "DELETE" -> new DeleteFactory(jsonMap);
			default -> throw new IllegalArgumentException("invalid method type: " + method);
		};
	}
	
	
	// -----------------------------------------------------
	
	
	/** */
	@Getter
	private Map<String, Object> jsonMap;
	
	
	/**
	 * 
	 * 
	 * @return
	 */
	public abstract MessageDTO genDTO();
	
	/**
	 * 생성자
	 * 
	 * @param jsonMap
	 */
	protected MessageFactory(Map<String, Object> jsonMap) {
		this.jsonMap = jsonMap;
	}
	
	/**
	 * 
	 * 
	 * @return
	 */
	protected String getKey() {
		
		if(this.jsonMap.containsKey("key") == false) {
			throw new IllegalArgumentException("'key' attribute is not found.");
		}
		
		return this.jsonMap.get("key").toString();
	}
	
	/**
	 * 
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Map<String, Object> getData() {
		
		if(this.jsonMap.containsKey("data") == false) {
			throw new IllegalArgumentException("'data' attribute is not found.");
		}
		
		return (Map<String, Object>)this.jsonMap.get("data");
	}
}
