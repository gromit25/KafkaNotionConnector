package com.octoby.kafka.notion.sink.process;

import com.octoby.kafka.notion.sink.domain.RowReqDTO;

import notion.api.v1.NotionClient;

/**
 * 
 * 
 */
public class PostReqProcess extends ReqProcess {

	@Override
	public void process(NotionClient client, RowReqDTO dto) {
	}
}
