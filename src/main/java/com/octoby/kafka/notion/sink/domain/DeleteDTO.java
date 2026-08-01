package com.octoby.kafka.notion.sink.domain;

import lombok.Getter;
import lombok.Setter;

/**
 * 
 * 
 * @author jmsohn
 */
public class DeleteDTO extends MessageDTO {
	
	
	/** */
	@Getter
	@Setter
	private String key;
	

	/**
	 * 생성자
	 */
	public DeleteDTO() {
		this.setMethod("DELETE");
	}
}
