package com.octoby.kafka.notion.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import com.octoby.kafka.notion.Constant;
import com.octoby.kafka.notion.util.CollectionUtil;
import com.octoby.kafka.notion.util.CronJob;
import com.octoby.kafka.notion.util.DateUtil;
import com.octoby.kafka.notion.util.JSONUtil;

import lombok.extern.slf4j.Slf4j;
import notion.api.v1.NotionClient;
import notion.api.v1.model.databases.DatabaseProperty.MultiSelect.Option;
import notion.api.v1.model.databases.QueryResults;
import notion.api.v1.model.pages.Page;
import notion.api.v1.model.pages.PageProperty;
import notion.api.v1.model.pages.PageProperty.RichText;
import notion.api.v1.request.databases.QueryDatabaseRequest;

/**
 * 노션 정보 수집 
 * 
 * @author jmsohn
 */
@Slf4j
public class NotionSourceTask extends SourceTask {
	
	
	/** 노션 토큰 값 */
	private String notionToken;
	
	/** 노션 크라이언트 객체 */
	private NotionClient client;
	
	/** 노션 DB 아이디 목록 */
	private List<String> dbList;
	
	/** 수집 스케줄러 객체 */
	private CronJob.CronExp scheduler;
	
	/** 이전 polling 시간 */
	private long lastPollTime = -1;
	
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
			this.notionToken = propMap.get(Constant.SOURCE_NOTION_TOKEN_PROPNAME);
			
			// 노션 클라이언트 객체 생성
			this.client = new NotionClient(this.notionToken);
			
			// 노션 DB 목록 객체 생성
			this.dbList = CollectionUtil.toList(
				propMap.get(Constant.SOURCE_NOTION_DB_LIST_PROPNAME)
			);
			
			// 수집 주기 객체 생성
			this.scheduler = CronJob.CronExp.create(
				propMap.get(Constant.SOURCE_POLL_SCHEDULE_PROPNAME)	
			);
			
			// 토픽 설정
			this.topic = propMap.get(Constant.SOURCE_TOPIC_PROPNAME);
			
		} catch(Exception ex) {
			
			log.error("notion source task error.", ex);
			
			this.client = null;
			this.scheduler = null;
		}
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		
		// 노션 클라이언트 또는 스케줄러가 설정되어 있지 않을 경우, 빈값 반환
		if(this.client == null || this.scheduler == null) {
			return List.of();
		}
		
		// 최초 실행일 경우 최종 폴링 시간을 현재시간으로 설정
		// 현재 시간과 다음 폴링 시간 사이의 데이터를 신규 데이터로 인식하기 위함
		if(this.lastPollTime < 0) {
			this.lastPollTime = System.currentTimeMillis();
		}
		
		// 다음 수집 시간까지 대기
		long nextTime = this.scheduler.getNextTimeInMillis();
		Thread.sleep(nextTime - System.currentTimeMillis());
		
		// DB 아이디 별 수집 실행 및 레코드 목록에 추가
		List<SourceRecord> recordList = new ArrayList<>();
		
		for(String dbId: this.dbList) {
			recordList.addAll(this.getNewRecordList(dbId));
		}

		// 최종 폴링 시간 설정
		this.lastPollTime = nextTime;
		
		return recordList;
	}
	
	/**
	 * 노션 DB의 새로운 레코드 목록을 반환
	 * 
	 * @param dbId 노션 DB 아이디
	 * @return 새로운 레코드 목록
	 */
	private List<SourceRecord> getNewRecordList(String dbId) {
				
		// db의 데이터를 모두 조회
		QueryDatabaseRequest request = new QueryDatabaseRequest(dbId);
		QueryResults results = client.queryDatabase(request);

		// 새로운 레코드 목록 저장 객체
		List<SourceRecord> newList = new ArrayList<>();
		
		for(Page row: results.getResults()) {
						
			// 새로운 레코드 여부 검사
			if(this.isNewRecord(row) == false) {
				continue;
			}
			
			// 새로운 레코드 객체 생성
			String message = toJSON(row);
			
			SourceRecord newRecord = new SourceRecord(
				null,					// 소스 파티션
				null,					// 소스 오프셋
				this.topic,				// 토픽
				Schema.STRING_SCHEMA,	// 메시지 스키마 설정
				message					// 전송 메시지
			);
			
			// 새로운 레코드 추가
			newList.add(newRecord);
		}
		
		return newList;
	}
	
	/**
	 * 새로운 레코드 여부 반환
	 * 
	 * @param row 노션 DB의 로우
	 * @return 새로운 레코드 여부
	 */
	private boolean isNewRecord(Page row) {
		
		try {
			return this.lastPollTime <= DateUtil.toMillis(row.getLastEditedTime());
		} catch(Exception ex) {
			return false;
		}
	}
	
	/**
	 * 노션 DB의 로우를 JSON 문자열로 변환 후 반환
	 * 
	 * @param row 노션 DB의 로우
	 * @return JSON 문자열
	 */
	private static String toJSON(Page row) {

		Map<String, Object> jsonMap = new HashMap<>();

		// 컬럼별로 JSON 데이터 설정
		Map<String, PageProperty> propMap = row.getProperties();
		
		for(String key: propMap.keySet()) {
			
			PageProperty prop = propMap.get(key);
			
			// 컬럼의 타입별로 데이터 설정
			String type = prop.getType().getValue();
			
			switch(type) {
			case "title":
				{
					StringBuilder buffer = new StringBuilder();
					
					for(RichText text: prop.getTitle()) {
						buffer.append(text.getText().getContent()).append("\n");
					}
					
					jsonMap.put(key, buffer.toString());
				}
				
				break;
				
			case "rich_text":
				{
					StringBuilder buffer = new StringBuilder();
					
					for(RichText text: prop.getRichText()) {
						buffer.append(text.getText().getContent()).append("\n");
					}
					
					jsonMap.put(key, buffer.toString());
				}
				
				break;
				
			case "number":
				jsonMap.put(key, prop.getNumber().doubleValue());
				break;
				
			case "date":
				jsonMap.put(
					key,
					Map.of(
						"start", prop.getDate().getStart(),
						"end", prop.getDate().getEnd(),
						"timezone", prop.getDate().getTimeZone()
					)
				);
				break;
				
			case "multi_select":
				{
					StringBuilder buffer = new StringBuilder();
					
					for(Option option: prop.getMultiSelect()) {
						buffer.append(option.getName()).append("\n");
					}
					
					jsonMap.put(key, buffer.toString());
				}

				break;
			}
		}
		
		// JSON으로 
		return JSONUtil.toJSON(jsonMap);
	}

	@Override
	public void stop() {
		
		// 노션 클라이언트 종료
		if(this.client != null) {
			this.client.close();
		}
	}
}
