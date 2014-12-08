/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.springframework.integration.kafka.simple.model;

import kafka.message.Message;

/**
 * @author Marius Bogoevici
 */
public class KafkaMessage {

	private Message message;

	private long offset;

	private long nextOffset;

	private long highWaterMark;

	private Partition partition;

	public KafkaMessage(Message message, long offset, long nextOffset, long highWaterMark, Partition partition) {
		this.message = message;
		this.offset = offset;
		this.nextOffset = nextOffset;
		this.highWaterMark = highWaterMark;
		this.partition = partition;
	}

	public Message getMessage() {
		return message;
	}

	public void setMessage(Message message) {
		this.message = message;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public long getNextOffset() {
		return nextOffset;
	}

	public long getHighWaterMark() {
		return highWaterMark;
	}

	public void setHighWaterMark(long highWaterMark) {
		this.highWaterMark = highWaterMark;
	}

	public void setNextOffset(long nextOffset) {
		this.nextOffset = nextOffset;
	}

	public Partition getPartition() {
		return partition;
	}

	public void setPartition(Partition partition) {
		this.partition = partition;
	}

}
