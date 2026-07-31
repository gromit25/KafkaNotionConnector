package com.octoby.kafka.notion.util;

import java.util.HashMap;
import java.util.Map;

/**
 * 프로퍼티 유틸리티 클래스
 * 
 * @author jmsohn
 */
public class PropertiesUtil {

	/**
	 * 프로퍼티 맵의 문자열 내 변수를 값으로 교체 함
	 * 
	 * @param propMap 프로퍼티 맵
	 * @param valueMap 변수 값 컨테이너
	 * @return 변수가 교체된 맵
	 */
	public static Map<String, String> resolve(Map<String, String> propMap, Map<String, String> valueMap) {
		
		if(propMap == null || propMap.size() == 0) {
			return Map.of();
		}
		
		if(valueMap == null || valueMap.size() == 0) {
			return Map.of();
		}
		
		Map<String, String> resolvedMap = new HashMap<>();
		
		for(String key: propMap.keySet()) {
			resolvedMap.put(
				key,
				StringUtil.replaceVars(propMap.get(key), valueMap)
			);
		}
		
		return resolvedMap;
	}
	
	/**
	 * 프로퍼티 맵의 문자열 내 변수를 환경변수 값으로 교체 함
	 * 
	 * @param propMap 프로퍼티 맵
	 * @return 변수가 교체된 맵
	 */
	public static Map<String, String> resolve(Map<String, String> propMap) {
		return resolve(propMap, System.getenv());
	}
}
