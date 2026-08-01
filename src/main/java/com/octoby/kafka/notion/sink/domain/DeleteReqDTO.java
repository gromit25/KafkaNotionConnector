package com.octoby.kafka.notion.sink.domain;

import lombok.Getter;
import lombok.Setter;

/**
 * 노션 DB의 행 삭제 메시지 DTO 클래스
 * 
 * @author jmsohn
 */
public class DeleteReqDTO extends ReqDTO {
	
	
	/** 삭제할 행의 키 */
	@Getter
	@Setter
	private String key;
	

	/**
	 * 생성자
	 */
	public DeleteReqDTO() {
		this.setMethod("DELETE");
	}
}
