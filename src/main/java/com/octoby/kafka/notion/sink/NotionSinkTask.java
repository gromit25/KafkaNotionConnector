package com.octoby.kafka.notion.sink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import com.octoby.kafka.notion.Constant;
import com.octoby.kafka.notion.sink.domain.RowReqDTO;
import com.octoby.kafka.notion.sink.factory.ReqFactory;
import com.octoby.kafka.notion.sink.process.ReqProcess;
import com.octoby.kafka.notion.util.CollectionUtil;
import com.octoby.kafka.notion.util.CronJob;
import com.octoby.kafka.notion.util.JSONUtil;

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
	public void put(Collection<SinkRecord> sinkRecords) {
		
		// 레코드가 없을 경우 반환
		if(sinkRecords == null || sinkRecords.size() == 0) {
			return;
		}
		
		// 노션 클라이언트 객체가 없을 경우 반환
		if(this.client == null) {
			return;
		}
		
		// 각 레코드 별로 처리
		for(SinkRecord sinkRecord: sinkRecords) {
			
			if(sinkRecord.value() == null) {
				continue;
			}
			
			try {
				
				// 메시지의 팩토리 객체 생성
				ReqFactory factory = ReqFactory.create(sinkRecord.value().toString());
				
				// DTO 객체 생성
				RowReqDTO dto = factory.createDTO();
				
				// 프로세스 객체 생성
				ReqProcess process = factory.createProcess();
				
				// 처리 실행
				process.process(this.client, dto);
				
			} catch(Exception ex) {
				log.error("sink task error.", ex);
			}
		}
	}

	@Override
	public void stop() {
		
		// 노션 클라이언트 종료
		if(this.client != null) {
			this.client.close();
		}
	}
}
