package com.octoby.kafka.notion.sink.domain;

import java.util.Map;

import lombok.Getter;
import lombok.Setter;

/**
 * 노션 DB의 행 추가 메시지 DTO 클래스
 * 
 * @author jmsohn
 */
public class PostReqDTO extends RowReqDTO {
	

	/** 추가할 데이터 맵 */
	@Getter
	@Setter
	private Map<String, Object> dataMap;
	
	
	/**
	 * 생성자
	 */
	public PostReqDTO() {
		this.setMethod("POST");
	}
}
