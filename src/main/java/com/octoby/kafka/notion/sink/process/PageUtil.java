package com.octoby.kafka.notion.sink.process;

import java.util.HashMap;
import java.util.Map;

import com.octoby.kafka.notion.common.DBPageType;

import lombok.extern.slf4j.Slf4j;
import notion.api.v1.model.pages.PageProperty;

/**
 * 
 * 
 * @author jmsohn
 */
@Slf4j
class PageUtil {

	/**
	 * JSON 데이터 맵을 노션 속성 맵으로 전환 후 반환
	 * 
	 * @param data 데이터 맵
	 * @return 노션 속성 맵
	 */
	static Map<String, PageProperty> toPagePropertyMap(Map<String, Object> data) {
		
		// 노션 속성 맵 객체
		Map<String, PageProperty> pagePropertyMap = new HashMap<>();
		
		data.forEach((key, value) -> {
			
			try {
				
				// 데이터 타입 획득
				String typeName = data.get("type").toString();
				
				DBPageType type = DBPageType.get(typeName);
				if(type == null) {
					throw new Exception("unexpected type: " + typeName);
				}
				
				// 변환 후 저장
				pagePropertyMap.put(key, type.getPagePropery(data.get("value")));
				
			} catch(Exception ex) {
				
				log.error("page property(" + key + ") convert error.", ex);
			}
		});
	
		// 노션 속성 맵 반환
		return pagePropertyMap;
	}
}
