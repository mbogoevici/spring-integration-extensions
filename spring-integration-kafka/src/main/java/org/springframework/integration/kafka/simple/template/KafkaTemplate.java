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


package org.springframework.integration.kafka.simple.template;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.utility.ArrayIterate;
import com.gs.collections.impl.utility.LazyIterate;
import kafka.message.MessageAndOffset;

import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnection;
import org.springframework.integration.kafka.simple.connection.KafkaResolver;
import org.springframework.integration.kafka.simple.connection.KafkaResult;
import org.springframework.integration.kafka.simple.consumer.KafkaConfiguration;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageFetchRequest;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerAddress;
import org.springframework.integration.kafka.simple.consumer.KafkaMessage;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageBatch;
import org.springframework.integration.kafka.simple.offset.Offset;
import org.springframework.integration.kafka.simple.connection.Partition;


/**
 * @author Marius Bogoevici
 */
public class KafkaTemplate {

	private final KafkaResolver kafkaResolver;

	private KafkaConfiguration kafkaConfiguration;

	public KafkaTemplate(KafkaConfiguration kafkaConfiguration) {
		this.kafkaConfiguration = kafkaConfiguration;
		this.kafkaResolver = new KafkaResolver(kafkaConfiguration.getBrokerAddresses(), kafkaConfiguration.getPartitions().toArray(new Partition[kafkaConfiguration.getPartitions().size()]));
	}

	public List<KafkaBrokerConnection> getAllBrokers() {
		return new ArrayList<KafkaBrokerConnection>(kafkaResolver.resolveBrokers(kafkaConfiguration.getPartitions()).values());
	}

	public KafkaResolver getKafkaResolver() {
		return kafkaResolver;
	}

	public List<KafkaMessage> receive(KafkaBrokerAddress kafkaBrokerAddress, final Map<Partition, Offset> offsets, final int maxSize) {
		return this.receive(FastList.newList(kafkaResolver.resolvePartitions(kafkaBrokerAddress)).collect(new Function<Partition, KafkaMessageFetchRequest>() {
			@Override
			public KafkaMessageFetchRequest valueOf(Partition partition) {
				return new KafkaMessageFetchRequest(partition, offsets.get(partition).getOffset(), maxSize);
			}
		}).toTypedArray(KafkaMessageFetchRequest.class));
	}

	public List<KafkaMessage> receive(KafkaMessageFetchRequest... messageFetchRequests) {
		MutableList<KafkaBrokerAddress> distinctBrokerAddresses = ArrayIterate.collect(messageFetchRequests, new Function<KafkaMessageFetchRequest, KafkaBrokerAddress>() {
			@Override
			public KafkaBrokerAddress valueOf(KafkaMessageFetchRequest fetchRequest) {
				return kafkaResolver.resolveBroker(fetchRequest.getPartition()).getBrokerAddress();
			}
		}).distinct();
		if (distinctBrokerAddresses.size() != 1) {
			throw new IllegalArgumentException("All messages must be fetched from the same broker");
		}
		KafkaResult<KafkaMessageBatch> fetch = kafkaResolver.resolveAddress(distinctBrokerAddresses.get(0)).fetch(messageFetchRequests);
		if (fetch.getErrors().size() > 0) {
			// synchronously refresh on error
			kafkaResolver.refresh();
		}
		return FastList.newList(fetch.getResult().entrySet()).flatCollect(new Function<Map.Entry<Partition, KafkaMessageBatch>, Iterable<KafkaMessage>>() {
			@Override
			public Iterable<KafkaMessage> valueOf(final Map.Entry<Partition, KafkaMessageBatch> mapEntry) {
				return LazyIterate.collect(mapEntry.getValue().getMessageSet(), new Function<MessageAndOffset, KafkaMessage>() {
					@Override
					public KafkaMessage valueOf(MessageAndOffset object) {
						return new KafkaMessage(object.message(), object.offset(), object.nextOffset(), mapEntry.getValue().getHighWatermark(), mapEntry.getKey());
					}
				});
			}
		});
	}

}
