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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;

import org.springframework.integration.context.IntegrationObjectSupport;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.kafka.serializer.common.StringDecoder;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

/**
 * @author Marius Bogoevici
 */
public class KafkaSimpleConsumerMessageSource extends IntegrationObjectSupport implements MessageSource<Map<String, Map<Integer, List<Object>>>> {

	private String clientId;

	private final KafkaTemplate kafkaTemplate;

	private final TopicAndPartition topicAndPartition;

	private final Decoder decoder = new StringDecoder();

	private long offset;

	public KafkaSimpleConsumerMessageSource(BrokerAddress brokerAddress, String topic, int partition, long startReferenceDate) {
		topicAndPartition = new TopicAndPartition(topic, partition);
		kafkaTemplate = new KafkaTemplate(new KafkaConfiguration(Collections.singletonList(brokerAddress), Collections.singletonList(topicAndPartition)));
		offset = kafkaTemplate.getKafkaResolver().getAdminBroker().fetchInitialOffset(topicAndPartition, startReferenceDate).getResult().get(topicAndPartition);
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	@Override
	public Message<Map<String, Map<Integer, List<Object>>>> receive() {
		Iterable<MessageAndOffset> receive = kafkaTemplate.receive(topicAndPartition, offset);

		Map<String, Map<Integer, List<Object>>> responsePayload = new HashMap<String, Map<Integer, List<Object>>>();

		HashMap<Integer, List<Object>> partitionContent = new HashMap<Integer, List<Object>>();

		partitionContent.put(topicAndPartition.partition(), new ArrayList<Object>());
		responsePayload.put(topicAndPartition.topic(), partitionContent);

		for (MessageAndOffset messageAndOffset : receive) {
			byte[] dst = new byte[messageAndOffset.message().payloadSize()];
			messageAndOffset.message().payload().get(dst);
			responsePayload.get(topicAndPartition.topic()).get(topicAndPartition.partition()).add(decoder.fromBytes(dst));
			offset = messageAndOffset.nextOffset();
		}

		return MessageBuilder.withPayload(responsePayload).build();
	}
}
