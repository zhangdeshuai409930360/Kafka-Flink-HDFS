package com.flink.etl;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EventTimeBucketAssigner implements BucketAssigner<String, String> {
	private ObjectMapper mapper = new ObjectMapper();

	@Override
	public String getBucketId(String element, Context context) {
		JsonNode node = null;
		long date = System.currentTimeMillis();
		try {
			node = mapper.readTree(element);
//			date = (long) (node.path("timestamp").floatValue() * 1000);
			date = (long) (node.path("timestamp").floatValue());
		} catch (IOException e) {
			e.printStackTrace();
		}
//		String partitionValue = new SimpleDateFormat("yyyyMMddHHmm").format(new Date(date)); 
		String partitionValue = new SimpleDateFormat("yyyyMMdd").format(new Date(date));
		return "dt=" + partitionValue;
	}

	@Override
	public SimpleVersionedSerializer<String> getSerializer() {
		return SimpleVersionedStringSerializer.INSTANCE;
	}
}