package com.octoby.kafka.notion.sink;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import com.octoby.kafka.notion.Constant;
import com.octoby.kafka.notion.util.PropertiesUtil;

/**
 * 
 * 
 * @author jmsohn
 */
public class NotionSinkConnector extends SinkConnector {
	
	
	/** 카프카 커넥터 설정 정보 */
	private Map<String, String> configMap;
	

	@Override
	public String version() {
		return Constant.VERSION;
	}
	
	@Override
	public ConfigDef config() {
		return null;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}
}
