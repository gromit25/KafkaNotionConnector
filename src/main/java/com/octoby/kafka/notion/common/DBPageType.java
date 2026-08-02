package com.octoby.kafka.notion.common;

import java.util.Map;

import notion.api.v1.model.databases.DatabaseProperty.MultiSelect.Option;
import notion.api.v1.model.pages.PageProperty;
import notion.api.v1.model.pages.PageProperty.RichText;

/**
 * DB 페이지 타입<br>
 * 현재 버전은 일부만 추가되어 있음<br>
 * 추후 자속적으로 추가
 * 
 * @author jmsohn
 */
public enum DBPageType {

	TITLE("title") {
		
		@Override
		public Object getContents(PageProperty prop) {
			
			StringBuilder buffer = new StringBuilder();
			
			for(RichText text: prop.getTitle()) {
				buffer.append(text.getText().getContent()).append("\n");
			}
			
			return buffer.toString();
		}
	},
	
	RICH_TEXT("rich_text") {
		
		@Override
		public Object getContents(PageProperty prop) {
			
			StringBuilder buffer = new StringBuilder();
			
			for(RichText text: prop.getRichText()) {
				buffer.append(text.getText().getContent()).append("\n");
			}
			
			return buffer.toString();
		}
	},
	
	NUMBER("number") {
		
		@Override
		public Object getContents(PageProperty prop) {
			return prop.getNumber().doubleValue();
		}
	},
	
	DATE("date") {
		
		@Override
		public Object getContents(PageProperty prop) {
			return Map.of(
				"start", prop.getDate().getStart(),
				"end", prop.getDate().getEnd(),
				"timezone", prop.getDate().getTimeZone()
			);
		}
	},
	
	MULTI_SELECT("multi_select") {
		
		@Override
		public Object getContents(PageProperty prop) {
			
			StringBuilder buffer = new StringBuilder();
			
			for(Option option: prop.getMultiSelect()) {
				buffer.append(option.getName()).append("\n");
			}
			
			return buffer.toString();
		}
	}
	;
	
	// -------------------------
	
	
	/** 페이지 타입 이름 */
	private String name;
	
	
	/**
	 * 생성자
	 * 
	 * @param name 페이지 타임 이름
	 */
	DBPageType(String name) {
		this.name = name;
	}
	
	/**
	 * 주어진 이름에 해당하는 페이지 타입 반환
	 * 
	 * @param name 이름
	 * @return 페이지 타입
	 */
	public static DBPageType get(String name) {
		
		for(DBPageType type: DBPageType.values()) {
			if(type.name.equals(name) == true) {
				return type;
			}
		}
		
		return null;
	}
	
	/**
	 * 노션 페이지 속성 객체에서 컨텐츠 추출
	 * 
	 * @param prop 노션 페이지 속성 객체
	 * @return 컨텐츠
	 */
	public abstract Object getContents(PageProperty prop);
}
