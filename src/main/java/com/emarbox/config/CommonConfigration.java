package com.emarbox.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CommonConfigration {

	@Value("${emar.bs.consumer.record.separator}")
	private String recordSeparator;

	@Value("${emar.bs.consumer.buffer.size}")
	private int bufferSize;

	@Value("${emar.bs.consumer.buffer.time}")
	private int bufferTime;

	@Value("${emar.bs.consumer.redis.key}")
	private String redisKey;

	@Value("${emar.bs.consumer.batch.write.size}")
	private int batchWriteSize;

	@Value("${emar.bs.consumer.topic}")
	private String topicList;

	public String getRecordSeparator() {
		return recordSeparator;
	}

	public void setRecordSeparator(String recordSeparator) {
		this.recordSeparator = recordSeparator;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public int getBufferTime() {
		return bufferTime;
	}

	public void setBufferTime(int bufferTime) {
		this.bufferTime = bufferTime;
	}

	public String getRedisKey() {
		return redisKey;
	}

	public void setRedisKey(String redisKey) {
		this.redisKey = redisKey;
	}

	public int getBatchWriteSize() {
		return batchWriteSize;
	}

	public void setBatchWriteSize(int batchWriteSize) {
		this.batchWriteSize = batchWriteSize;
	}

	public String getTopicList() {
		return topicList;
	}

	public void setTopicList(String topicList) {
		this.topicList = topicList;
	}

}
