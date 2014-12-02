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


package org.springframework.integration.kafka.kafkasimpleconsumer;

import java.util.HashMap;
import java.util.Map;

import kafka.common.TopicAndPartition;

/**
 * @author Marius Bogoevici
 */
public class KafkaResultBuilder<T> {

	private Map<TopicAndPartition, T> result;

	private Map<TopicAndPartition, Short> errors;

	public KafkaResultBuilder() {
		this.result = new HashMap<TopicAndPartition, T>();
		this.errors = new HashMap<TopicAndPartition, Short>();
	}

	public KafkaResultBuilderTopicAndPartition add(TopicAndPartition topicAndPartition) {
		return new KafkaResultBuilderTopicAndPartition(topicAndPartition);
	}

	public KafkaResult<T> build() {
		return new KafkaResult(result, errors);
	}

	public class KafkaResultBuilderTopicAndPartition<T1 extends T> {

		private TopicAndPartition topicAndPartition;

		public KafkaResultBuilderTopicAndPartition(TopicAndPartition topicAndPartition) {
			this.topicAndPartition = topicAndPartition;
		}

		public KafkaResultBuilder withResult(T1 result) {
			if (KafkaResultBuilder.this.errors.containsKey(topicAndPartition)) {
				throw new IllegalArgumentException("A KafkaResult cannot contain both an error and a result for the same topic and partition");
			}
			KafkaResultBuilder.this.result.put(topicAndPartition, result);
			return KafkaResultBuilder.this;
		}

		public KafkaResultBuilder withError(short error) {
			if (KafkaResultBuilder.this.result.containsKey(topicAndPartition)) {
				throw new IllegalArgumentException("A FetchResult cannot contain both an error and a MessageSet for the same topic and partition");
			}
			KafkaResultBuilder.this.errors.put(topicAndPartition, error);
			return KafkaResultBuilder.this;
		}

	}
}
