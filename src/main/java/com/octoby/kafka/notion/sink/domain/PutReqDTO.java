package com.octoby.kafka.notion.sink.domain;

import java.util.Map;

import lombok.Getter;
import lombok.Setter;

/**
 * 노션 DB의 행 추가/수장 메시지 DTO 클래스
 * 
 * @author jmsohn
 */
public class PutReqDTO extends RowReqDTO {
	
	
	/** 추가/수정할 키 */
	@Getter
	@Setter
	private String key;

	/** 추가/수정할 데이터 맵 */
	@Getter
	@Setter
	private Map<String, Object> dataMap;
	
	
	/**
	 * 생성자
	 */
	public PutReqDTO() {
		this.setMethod("PUT");
	}
}
