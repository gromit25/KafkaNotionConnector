package com.octoby.kafka.notion;

/**
 * 상수 클래스
 * 
 * @author jmsohn
 */
public class Constant {

	/** 버전 */
	public static final String VERSION = "1.0";
	
	
	// 환경변수 관련 상수
	
	/** 노션 토큰 환경변수 명 */
	public static final String NOTION_TOKEN_ENVNAME = "OCTO_NOTION_TOKEN";
	
	/** 노션 대상 DB 목록 환경변수 명 */
	public static final String NOTION_DB_LIST_ENVNAME = "OCTO_NOTION_DB_LIST";
	
	/** 수집 스케줄 환경변수 명 */
	public static final String SCHEDULE_ENVNAME = "OCTO_SOURCE_SCHEDULE";
	
	/** 소스 토픽명 환경변수 명 */
	public static final String SOURCE_TOPIC_ENVNAME = "OCTO_SOURCE_TOPIC";
	

	// 설정 객체 내의 키
	
	/** 설정에서 노션 토큰 키 */
	public static final String NOTION_TOKEN_KEY = "NOTION_TOKEN";
	
	/** 노션 DB 목록 키*/
	public static final String NOTION_DB_LIST_KEY = "NOTION_DB_LIST";
	
	/** 노션 데이터 수집 스케줄 키 */
	public static final String SCHEDULE_KEY = "SOURCE_SCHEDULE";
	
	/** 카프카 토픽 키 */
	public static final String SOURCE_TOPIC_KEY = "SOURCE_TOPIC";
}
