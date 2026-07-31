package com.octoby.kafka.notion.sink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import com.octoby.kafka.notion.Constant;

/**
 * 
 * 
 * @author jmsohn
 */
public class NotionSinkTask extends SinkTask {

	@Override
	public String version() {
		return Constant.VERSION;
	}

	@Override
	public void start(Map<String, String> props) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}
}
