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

import java.util.ArrayList;
import java.util.List;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.list.mutable.FastList;
import kafka.common.ErrorMapping;
import kafka.javaapi.message.MessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;


/**
 * @author Marius Bogoevici
 */
public class KafkaTemplate {

	private final KafkaResolver kafkaResolver;

	private KafkaConfiguration kafkaConfiguration;

	public KafkaTemplate(KafkaConfiguration kafkaConfiguration) {
		this.kafkaConfiguration = kafkaConfiguration;
		this.kafkaResolver = new KafkaResolver(kafkaConfiguration.getBrokerAddresses());
	}

	public List<KafkaBrokerConnection> getAllBrokers() {
		return new ArrayList<KafkaBrokerConnection>(kafkaResolver.resolveBrokers(kafkaConfiguration.getPartitions()).values());
	}

	public KafkaResolver getKafkaResolver() {
		return kafkaResolver;
	}

	public void exec() {

	}

	public void send(Partition Partition, Message message) {

	}

	public void convertAndSend() {
	}

	public Iterable<KafkaMessage> receive(final Partition partition, long offset, int maxSize) {
		KafkaResult<MessageSet> fetch = kafkaResolver.resolveBroker(partition).fetch(new KafkaMessageFetchRequest(partition, offset, maxSize));
		if (fetch.getErrors().size() > 0) {
			throw new RuntimeException(ErrorMapping.exceptionFor(fetch.getErrors().values().iterator().next()));
		}
		MessageSet messageSet = fetch.getResult().get(partition);
		return FastList.newList(messageSet).collect(new Function<MessageAndOffset, KafkaMessage>() {
			@Override
			public KafkaMessage valueOf(MessageAndOffset messageAndOffset) {
				return new KafkaMessage(messageAndOffset.message(), messageAndOffset.nextOffset(), partition);
			}
		});
	}

	public <T> List<T> receiveAndConvert(Partition partition, long offset) {
		return null;
	}

}
