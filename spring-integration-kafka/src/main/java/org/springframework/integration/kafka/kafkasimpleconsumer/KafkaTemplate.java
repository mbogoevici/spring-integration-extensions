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

import java.util.List;
import java.util.Map;

import kafka.common.TopicAndPartition;
import kafka.javaapi.message.MessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;


/**
 * @author Marius Bogoevici
 */
public class KafkaTemplate {

	private final KafkaResolver kafkaResolver;

	public KafkaTemplate(KafkaConfiguration kafkaConfiguration) {
		this.kafkaResolver = new KafkaResolver(kafkaConfiguration);
	}

	public List<KafkaBrokerConnection> getAllBrokers() {
		return null;
	}

	public KafkaResolver getKafkaResolver() {
		return kafkaResolver;
	}

	public void exec() {

	}

	public void send(TopicAndPartition topicAndPartition, Message message) {

	}

	public void convertAndSend() {}

	public Iterable<MessageAndOffset> receive(TopicAndPartition topicAndPartition, long offset) {
		KafkaResult<MessageSet> fetch = kafkaResolver.resolveBroker(topicAndPartition).fetch(new FetchTarget(topicAndPartition, offset));
		MessageSet messageSet = fetch.getResult().get(topicAndPartition);
		return messageSet;
	}

	public <T> List<T> receiveAndConvert(TopicAndPartition topicAndPartition, long offset) {
		return null;
	}

}
