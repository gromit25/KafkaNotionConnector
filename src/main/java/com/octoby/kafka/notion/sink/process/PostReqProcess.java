package com.octoby.kafka.notion.sink.process;

import com.octoby.kafka.notion.sink.domain.ReqDTO;

import notion.api.v1.NotionClient;
import notion.api.v1.model.databases.DatabaseParent;
import notion.api.v1.model.databases.DatabaseParentType;
import notion.api.v1.model.pages.PageParent;
import notion.api.v1.request.pages.CreatePageRequest;

/**
 * 
 * 
 */
public class PostReqProcess extends ReqProcess {

	@Override
	public void process(NotionClient client, ReqDTO dto) {
		
		PageParent parent = PageParent.page(dto.getDbId());
		
		CreatePageRequest req = new CreatePageRequest(parent, null);
	}
}
