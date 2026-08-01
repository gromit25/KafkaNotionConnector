package com.octoby.kafka.notion;

/**
 * 상수 클래스
 * 
 * @author jmsohn
 */
public class Constant {

	/** 버전 */
	public static final String VERSION = "1.0";
	
	
	// 노션 속성 설정 관련 상수

	/** 노션 토큰 속성 설정 명 */
	public static final String NOTION_TOKEN_PROPNAME = "notion.token";
	
	/** 수집 대상 노션 DB 목록 속성 설정 명 */
	public static final String NOTION_DB_LIST_PROPNAME = "notion.db.list";

	
	// 소스 커넥터 속성 설정 관련 상수
	
	/** 소스 수집 스케줄 속성 설정 명 */
	public static final String SOURCE_POLL_SCHEDULE_PROPNAME = "source.poll.schedule";
	
	/** 소스 토픽명 속성 설정 명 */
	public static final String SOURCE_TOPIC_PROPNAME = "source.topic";
	
	// 싱크 커넥터 속성 설정 관련 상수
	
	/** */
	public static final String SINK_TOPIC_PROPNAME = "sink.topic";
}
