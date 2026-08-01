package com.octoby.kafka.notion.sink.domain;

import lombok.Getter;
import lombok.Setter;

/**
 * 노션 DB 메시지 DTO 클래스
 * 
 * @author jmsohn
 */
public class RowReqDTO {

	/** 처리 방식 */
	@Getter
	@Setter
	private String method;
	
	/** 노션 대상 DB 아이디 */
	@Getter
	@Setter
	private String dbId;
}
