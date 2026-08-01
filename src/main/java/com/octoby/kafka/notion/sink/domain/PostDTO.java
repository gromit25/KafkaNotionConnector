package com.octoby.kafka.notion.sink.domain;

import java.util.Map;

import lombok.Getter;
import lombok.Setter;

/**
 * 
 * 
 * @author jmsohn
 */
public class PostDTO extends MessageDTO {
	

	/** */
	@Getter
	@Setter
	private Map<String, Object> dataMap;
	
	
	/**
	 * 생성자
	 */
	public PostDTO() {
		this.setMethod("POST");
	}
}
