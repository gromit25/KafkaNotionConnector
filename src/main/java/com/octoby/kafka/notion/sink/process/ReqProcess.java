package com.octoby.kafka.notion.sink.process;

import com.octoby.kafka.notion.sink.domain.ReqDTO;

import notion.api.v1.NotionClient;

/**
 * 
 * 
 * @author jmsohn
 */
public abstract class ReqProcess {

	/**
	 * 
	 * 
	 * @param client
	 * @param dto
	 */
	public abstract void process(NotionClient client, ReqDTO dto);
}
