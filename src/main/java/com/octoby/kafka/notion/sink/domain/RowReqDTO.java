package com.octoby.kafka.notion.sink.domain;

import lombok.Getter;
import lombok.Setter;

/**
 * 노션 DB 메시지 DTO 클래스
 * 
 * @author jmsohn
 */
public class RowReqDTO {

	/** */
	@Getter
	@Setter
	private String method;
}
