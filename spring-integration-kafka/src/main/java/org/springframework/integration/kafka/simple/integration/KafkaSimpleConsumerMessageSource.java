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


package org.springframework.integration.kafka.simple.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.serializer.Decoder;

import org.springframework.integration.context.IntegrationObjectSupport;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.kafka.simple.consumer.KafkaConfiguration;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageFetchRequest;
import org.springframework.integration.kafka.simple.consumer.KafkaTemplate;
import org.springframework.integration.kafka.simple.model.Partition;
import org.springframework.integration.kafka.simple.model.KafkaBrokerAddress;
import org.springframework.integration.kafka.simple.model.KafkaMessage;
import org.springframework.integration.kafka.serializer.common.StringDecoder;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

/**
 * @author Marius Bogoevici
 */
public class KafkaSimpleConsumerMessageSource extends IntegrationObjectSupport implements MessageSource<Map<String, Map<Integer, List<Object>>>> {

	private String clientId;

	private final KafkaTemplate kafkaTemplate;

	private final Partition partition;

	private final Decoder decoder = new StringDecoder();

	private long offset;

	private int maxSize = 10000;

	public KafkaSimpleConsumerMessageSource(KafkaBrokerAddress kafkaBrokerAddress, String topic, int partition, long startReferenceDate) {
		this.partition = new Partition(topic, partition);
		kafkaTemplate = new KafkaTemplate(new KafkaConfiguration(Collections.singletonList(kafkaBrokerAddress), Collections.singletonList(this.partition)));
		offset = kafkaTemplate.getKafkaResolver().getAdminBroker().fetchInitialOffset(startReferenceDate, this.partition).getResult().get(this.partition);
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	@Override
	public Message<Map<String, Map<Integer, List<Object>>>> receive() {
		Iterable<KafkaMessage> receivedMessages = kafkaTemplate.receive(new KafkaMessageFetchRequest(partition, offset, maxSize));

		Map<String, Map<Integer, List<Object>>> responsePayload = new HashMap<String, Map<Integer, List<Object>>>();

		HashMap<Integer, List<Object>> partitionContent = new HashMap<Integer, List<Object>>();

		partitionContent.put(partition.getNumber(), new ArrayList<Object>());
		responsePayload.put(partition.getTopic(), partitionContent);

		for (KafkaMessage message : receivedMessages) {
			byte[] dst = new byte[message.getMessage().payloadSize()];
			message.getMessage().payload().get(dst);
			responsePayload.get(partition.getTopic()).get(partition.getTopic()).add(decoder.fromBytes(dst));
			offset = message.getNextOffset();
		}

		return MessageBuilder.withPayload(responsePayload).build();
	}
}
