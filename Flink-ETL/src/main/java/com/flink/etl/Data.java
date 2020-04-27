package com.flink.etl;

import lombok.AllArgsConstructor;
import lombok.ToString;

@lombok.Data
@ToString

public class Data {
	
	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	private long timestamp;
	private String event;
	private String uuid;
 
	public Data(long timestamp, String event, String uuid) {
		
		this.timestamp=timestamp;
		this.event=event;
		this.uuid=uuid;
	}

}
