package com.octoby.kafka.notion.sink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import com.octoby.kafka.notion.Constant;
import com.octoby.kafka.notion.util.CollectionUtil;
import com.octoby.kafka.notion.util.CronJob;

import lombok.extern.slf4j.Slf4j;
import notion.api.v1.NotionClient;

/**
 * 
 * 
 * @author jmsohn
 */
@Slf4j
public class NotionSinkTask extends SinkTask {
	
	
	/** 노션 토큰 값 */
	private String notionToken;
	
	/** 노션 크라이언트 객체 */
	private NotionClient client;
	
	/** 전송할 토픽명 */
	private String topic;
	

	@Override
	public String version() {
		return Constant.VERSION;
	}

	@Override
	public void start(Map<String, String> propMap) {
		
		try {
			
			// 노션 토큰 설정
			this.notionToken = propMap.get(Constant.NOTION_TOKEN_PROPNAME);
			
			// 노션 클라이언트 객체 생성
			this.client = new NotionClient(this.notionToken);
			
			// 토픽 설정
			this.topic = propMap.get(Constant.SINK_TOPIC_PROPNAME);
			
		} catch(Exception ex) {
			
			log.error("notion sink task error.", ex);
			
			this.client = null;
		}
	}

	@Override
	public void put(Collection<SinkRecord> records) {
	}

	@Override
	public void stop() {
	}
}
