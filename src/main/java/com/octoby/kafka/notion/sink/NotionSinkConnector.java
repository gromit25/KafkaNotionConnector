package com.octoby.kafka.notion.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import com.octoby.kafka.notion.Constant;
import com.octoby.kafka.notion.util.PropertiesUtil;

/**
 * 노션 싱크 커넥터 클래스
 * 
 * @author jmsohn
 */
public class NotionSinkConnector extends SinkConnector {
	
	
	/** 카프카 싱크 커넥터 설정 정보 */
	private Map<String, String> configMap;
	

	@Override
	public String version() {
		return Constant.VERSION;
	}
	
	@Override
	public ConfigDef config() {
		
		return new ConfigDef()
			.define(
				Constant.NOTION_TOKEN_PROPNAME,
				ConfigDef.Type.STRING,
				ConfigDef.Importance.HIGH,
				"노션 API 토큰"
			)
			.define(
				Constant.NOTION_DB_LIST_PROPNAME,
				ConfigDef.Type.STRING,
				ConfigDef.Importance.HIGH,
				"노션 DB 아이디 목록"
			)
			;
	}

	@Override
	public void start(Map<String, String> propMap) {
		this.configMap = PropertiesUtil.resolve(propMap);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return NotionSinkTask.class;
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
}
