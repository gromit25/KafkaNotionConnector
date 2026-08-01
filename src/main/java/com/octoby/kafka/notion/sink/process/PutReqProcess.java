package com.octoby.kafka.notion.sink.process;

import com.octoby.kafka.notion.sink.domain.RowReqDTO;

import notion.api.v1.NotionClient;

/**
 * 
 * 
 * @author jmsohn
 */
public class PutReqProcess extends ReqProcess {

	@Override
	public void process(NotionClient client, RowReqDTO dto) {
	}
}
