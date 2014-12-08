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

import kafka.javaapi.message.MessageSet;

/**
 * @author Marius Bogoevici
 */
public class KafkaMessageBatch {

	private Partition partition;

	private MessageSet messageSet;

	private long highWatermark;

	public KafkaMessageBatch(Partition partition, MessageSet messageSet, long highWatermark) {
		this.partition = partition;
		this.messageSet = messageSet;
		this.highWatermark = highWatermark;
	}

	public Partition getPartition() {
		return partition;
	}

	public void setPartition(Partition partition) {
		this.partition = partition;
	}

	public MessageSet getMessageSet() {
		return messageSet;
	}

	public void setMessageSet(MessageSet messageSet) {
		this.messageSet = messageSet;
	}

	public long getHighWatermark() {
		return highWatermark;
	}

	public void setHighWatermark(long highWatermark) {
		this.highWatermark = highWatermark;
	}
}
