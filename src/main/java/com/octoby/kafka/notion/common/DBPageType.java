package com.octoby.kafka.notion.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import notion.api.v1.model.common.RichTextType;
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
			
			List<String> textList = new ArrayList<String>();
			
			for(RichText text: prop.getTitle()) {
				textList.add(text.getText().getContent());
			}
			
			return textList;
		}

		@Override
		public PageProperty getPagePropery(Object contents) {
			
			if(contents == null) {
				contents = "null";
			}
			
			PageProperty titleProp = new PageProperty();
			titleProp.setTitle(List.of(
			    new PageProperty.RichText(
			    	RichTextType.Text,
			    	new PageProperty.RichText.Text(contents.toString())
			    )
			));
			
			return titleProp;
		}
	},
	
	RICH_TEXT("rich_text") {
		
		@Override
		public Object getContents(PageProperty prop) {
			
			List<String> textList = new ArrayList<String>();
			
			for(RichText text: prop.getTitle()) {
				textList.add(text.getText().getContent());
			}
			
			return textList;
		}

		@Override
		public PageProperty getPagePropery(Object contents) {
			
			if(contents == null) {
				contents = "null";
			}
			
			PageProperty richTextProp = new PageProperty();
			richTextProp.setRichText(List.of(
			    new PageProperty.RichText(
			    	RichTextType.Text,
			    	new PageProperty.RichText.Text(contents.toString())
			    )
			));
			
			return richTextProp;
		}
	},
	
	NUMBER("number") {
		
		@Override
		public Object getContents(PageProperty prop) {
			return prop.getNumber();
		}

		@Override
		public PageProperty getPagePropery(Object contents) {
			
			if(contents == null || contents instanceof Number == false) {
				contents = Double.NaN;
			}
			
			PageProperty numberProp = new PageProperty();
			numberProp.setNumber((Number)contents);
			
			return numberProp;
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

		@Override
		public PageProperty getPagePropery(Object contents) {
			
			//
			@SuppressWarnings("unchecked")
			Map<String, Object> contentsMap = (Map<String, Object>)contents;
			
			//
			PageProperty.Date date = new PageProperty.Date();
			
			if(contentsMap.containsKey("start") == true) {
				date.setStart(contentsMap.get("start").toString());
			}
			
			if(contentsMap.containsKey("end") == true) {
				date.setEnd(contentsMap.get("end").toString());
			}
			
			if(contentsMap.containsKey("timezone") == true) {
				date.setTimeZone(contentsMap.get("timezone").toString());
			}
			
			//
			PageProperty dateProp = new PageProperty();
			dateProp.setDate(date);
			
			return dateProp;
		}
	},
	
	MULTI_SELECT("multi_select") {
		
		@Override
		public Object getContents(PageProperty prop) {
			
			List<String> textList = new ArrayList<String>();
			
			for(RichText text: prop.getTitle()) {
				textList.add(text.getText().getContent());
			}
			
			return textList;
		}

		@Override
		public PageProperty getPagePropery(Object contents) {
			
			//
			@SuppressWarnings("unchecked")
			List<String> optionIdList = (List<String>)contents;
			
			//
			List<Option> optionList = new ArrayList<>();
			
			for(String optionId: optionIdList) {
				
				Option option = new Option();
				option.setId(optionId);
				
				optionList.add(option);
			}
			
			//
			PageProperty multiSelectProp = new PageProperty();
			multiSelectProp.setMultiSelect(optionList);
			
			return multiSelectProp;
		}
	}
	;
	
	// -------------------------
	
	
	/** 페이지 타입 이름 */
	private String pageTypeName;
	
	
	/**
	 * 생성자
	 * 
	 * @param pageTypeName 페이지 타임 이름
	 */
	DBPageType(String pageTypeName) {
		this.pageTypeName = pageTypeName;
	}
	
	/**
	 * 주어진 이름에 해당하는 페이지 타입 반환
	 * 
	 * @param pageTypeName 페이지 타입 이름
	 * @return 페이지 타입
	 */
	public static DBPageType get(String pageTypeName) {
		
		for(DBPageType type: DBPageType.values()) {
			if(type.pageTypeName.equals(pageTypeName) == true) {
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
	
	/**
	 * 페이지 객체 반환
	 * 
	 * @param object 컨텐츠 객체 
	 * @return 페이지 객체
	 */
	public abstract PageProperty getPagePropery(Object contents);
}
