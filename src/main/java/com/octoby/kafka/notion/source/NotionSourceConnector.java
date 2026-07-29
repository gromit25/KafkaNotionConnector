package com.octoby.kafka.notion.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import com.octoby.kafka.notion.Constant;
import com.octoby.kafka.notion.util.StringUtil;

/**
 * 노션 소스 커넥터 클래스
 * 
 * @author jmsohn
 */
public class NotionSourceConnector extends SourceConnector {
	
	
	/** 카프카 커넥터 설정 정보 */
	private Map<String, String> configMap;
	
	
	@Override
	public String version() {
		return Constant.VERSION;
	}

	@Override
	public void start(Map<String, String> propMap) {
		
		this.configMap = new HashMap<>();
		
		// 카프카 커넥트 기본설정 값 추가
		this.configMap.putAll(propMap);
		
		// 노션 토큰 설정
		this.configMap.put(
			Constant.NOTION_TOKEN_KEY,
			getEnv(Constant.NOTION_TOKEN_ENVNAME)
		);
		
		// 노션 DB 목록 설정
		this.configMap.put(
			Constant.NOTION_DB_LIST_KEY,
			getEnv(Constant.NOTION_DB_LIST_ENVNAME)
		);
		
		// 수집 설정 설정
		this.configMap.put(
			Constant.SCHEDULE_KEY,
			getEnv(Constant.SCHEDULE_ENVNAME)
		);
		
		// 소스 토픽 설정
		this.configMap.put(
			Constant.SOURCE_TOPIC_KEY,
			getEnv(Constant.SOURCE_TOPIC_ENVNAME)
		);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return NotionSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		
		List<Map<String, String>> taskConfigs = new ArrayList<>();
		
		for(int index = 0; index < maxTasks; index++) {
			taskConfigs.add(this.configMap);
		}
		
		return taskConfigs;
	}

	@Override
	public void stop() {
		// do nothing
	}
	
	@Override
	public ConfigDef config() {
		return null;
	}
	
	/**
	 * 환경변수 값 반환
	 * 
	 * @param name 환경변수 명
	 * @return 환경변수 값
	 */
	private static String getEnv(String name) {
		
		// 환경변수 명 빈값 여부 검사
		if(StringUtil.isBlank(name) == true) {
			throw new IllegalArgumentException("'name' is null or blank.");
		}
		
		// 환경변수 값 획득 및 빈값 여부 검사
		String value = System.getenv(name);
		if(StringUtil.isBlank(value) == true) {
			throw new RuntimeException(name + "'s value is null or blank.");
		}
		
		// 환경변수 값 반환
		return value;
	}
}
