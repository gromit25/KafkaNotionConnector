package com.octoby.kafka.notion.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * 컬렉션 유틸리티 클래스
 * 
 * @author jmsohn
 */
public class CollectionUtil {

	/**
	 * 대상 문자열을 컴마(,) 단위로 잘라서 목록 객체에 넣어 반환
	 * 
	 * @param listStr 대상 문자열
	 * @param supplier 목록 객체 생성 객체
	 * @return 목록 객체
	 */
	public static List<String> toList(String listStr, Supplier<List<String>> supplier) {
		
		if(StringUtil.isBlank(listStr) == true) {
			return List.of();
		}
		
		if(supplier == null) {
			throw new IllegalArgumentException("'supplier' is null.");
		}
		
		List<String> list = supplier.get();
		
		String[] splitedAry = listStr.split(" *, *");
		for(String splited: splitedAry) {
			list.add(splited);
		}
		
		return list;
	}
	
	/**
	 * 대상 문자열을 컴마(,) 단위로 잘라서 목록 객체(ArrayList)에 넣어 반환
	 * 
	 * @param listStr 대상 문자열
	 * @return 목록 객체
	 */
	public static List<String> toList(String listStr) {
		return toList(listStr, () -> new ArrayList<String>());
	}
}
