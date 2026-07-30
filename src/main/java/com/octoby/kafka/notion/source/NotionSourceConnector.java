package com.octoby.kafka.notion.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import com.octoby.kafka.notion.Constant;
import com.octoby.kafka.notion.util.CronJob;

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
	public ConfigDef config() {
		
		return new ConfigDef()
			.define(
				Constant.SOURCE_NOTION_TOKEN_PROPNAME,
				ConfigDef.Type.STRING,
				ConfigDef.Importance.HIGH,
				"노션 API 토큰"
			)
			.define(
				Constant.SOURCE_NOTION_DB_LIST_PROPNAME,
				ConfigDef.Type.STRING,
				ConfigDef.Importance.HIGH,
				"노션 DB 아이디 목록"
			)
			.define(
				Constant.SOURCE_POLL_SCHEDULE_PROPNAME,
				ConfigDef.Type.STRING,
				"*/10 * * * * *",
				(name, value) -> {
					if(CronJob.CronExp.isValid(value.toString()) == false) {
						new ConfigException(name, value, "invalid cron expression.");
					}
				},
				ConfigDef.Importance.HIGH,
				"수집 스케줄"
			)
			.define(
				Constant.SOURCE_TOPIC_PROPNAME,
				ConfigDef.Type.STRING,
				ConfigDef.Importance.HIGH,
				"소스 토픽명"
			)
			;
	}

	@Override
	public void start(Map<String, String> propMap) {
		this.configMap = propMap;
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
}
