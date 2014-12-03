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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.list.mutable.FastList;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.MessageSet;

/**
 * @author Marius Bogoevici
 */
public class KafkaBrokerConnection {

	public static final String DEFAULT_CLIENT_ID = "spring.integration.kafka";

	public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

	public static final int DEFAULT_SOCKET_TIMEOUT = 10000;

	public static final short KAFKA_CONSUMER_VERSION = kafka.api.OffsetRequest.CurrentVersion();

	public static final int MAX_NUM_OFFSETS = 1;

	public static final Function<Partition, TopicAndPartition> AS_KAFKA_TOPIC_AND_PARTITION = new Function<Partition, TopicAndPartition>() {
		@Override
		public TopicAndPartition valueOf(Partition object) {
			return new TopicAndPartition(object.getTopic(), object.getNumber());
		}
	};

	private final SimpleConsumer simpleConsumer;

	private final static AtomicInteger correlationIdCounter = new AtomicInteger(new Random(new Date().getTime()).nextInt());

	private KafkaBrokerAddress brokerAddress;

	public KafkaBrokerConnection(KafkaBrokerAddress brokerAddress) {
		this(brokerAddress, DEFAULT_CLIENT_ID);
	}

	public KafkaBrokerConnection(KafkaBrokerAddress brokerAddress, String clientId) {
		this(brokerAddress, clientId, DEFAULT_SOCKET_TIMEOUT, DEFAULT_BUFFER_SIZE);
	}

	public KafkaBrokerConnection(KafkaBrokerAddress brokerAddress, String clientId, int bufferSize, int soTimeout) {
		this.brokerAddress = brokerAddress;
		this.simpleConsumer = new SimpleConsumer(brokerAddress.getHost(), brokerAddress.getPort(), soTimeout, bufferSize, clientId);
	}

	public KafkaBrokerAddress getBrokerAddress() {
		return brokerAddress;
	}

	public KafkaResult<MessageSet> fetch(KafkaMessageFetchRequest kafkaMessageFetchRequest) {
		return fetch(Collections.singletonList(kafkaMessageFetchRequest));
	}

	public KafkaResult<MessageSet> fetch(List<KafkaMessageFetchRequest> kafkaMessageFetchRequests) {
		FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder();
		for (KafkaMessageFetchRequest kafkaMessageFetchRequest : kafkaMessageFetchRequests) {
			fetchRequestBuilder.addFetch(kafkaMessageFetchRequest.getPartition().getTopic(), kafkaMessageFetchRequest.getPartition().getNumber(), kafkaMessageFetchRequest.getOffset(), this.simpleConsumer.bufferSize());
		}
		FetchResponse fetchResponse = this.simpleConsumer.fetch(fetchRequestBuilder.build());
		KafkaResultBuilder<MessageSet> kafkaResultBuilder = new KafkaResultBuilder<MessageSet>();
		for (KafkaMessageFetchRequest kafkaMessageFetchRequest : kafkaMessageFetchRequests) {
			short errorCode = fetchResponse.errorCode(kafkaMessageFetchRequest.getPartition().getTopic(), kafkaMessageFetchRequest.getPartition().getNumber());
			if (ErrorMapping.NoError() == errorCode) {
				kafkaResultBuilder.add(kafkaMessageFetchRequest.getPartition()).withResult(fetchResponse.messageSet(kafkaMessageFetchRequest.getPartition().getTopic(), kafkaMessageFetchRequest.getPartition().getNumber()));
			}
			else {
				kafkaResultBuilder.add(kafkaMessageFetchRequest.getPartition()).withError(fetchResponse.errorCode(kafkaMessageFetchRequest.getPartition().getTopic(), kafkaMessageFetchRequest.getPartition().getNumber()));
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<Long> fetchOffsetforConsumer(Partition partition) {
		return fetchOffsetForConsumer(Collections.singletonList(partition));
	}

	public KafkaResult<Long> fetchOffsetForConsumer(List<Partition> partitions) {
		FastList<TopicAndPartition> topicsAndPartitions = FastList.newList(partitions).collect(AS_KAFKA_TOPIC_AND_PARTITION);
		OffsetFetchRequest offsetFetchRequest = new OffsetFetchRequest(simpleConsumer.clientId(), topicsAndPartitions, KAFKA_CONSUMER_VERSION, createCorrelationId(), simpleConsumer.clientId());
		OffsetFetchResponse offsetFetchResponse = simpleConsumer.fetchOffsets(offsetFetchRequest);
		KafkaResultBuilder<Long> kafkaResultBuilder = new KafkaResultBuilder<Long>();
		for (Partition partition : partitions) {
			OffsetMetadataAndError offsetMetadataAndError = offsetFetchResponse.offsets().get(partition);
			short errorCode = offsetMetadataAndError.error();
			if (ErrorMapping.NoError() == errorCode) {
				kafkaResultBuilder.add(partition).withResult(offsetMetadataAndError.offset());
			}
			else {
				kafkaResultBuilder.add(partition).withError(offsetMetadataAndError.error());
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<Long> fetchInitialOffset(Partition Partition, long time) {
		return fetchInitialOffset(Collections.singletonList(Partition), time);
	}

	public KafkaResult<Long> fetchInitialOffset(List<Partition> topicsAndPartitions, long time) {
		Map<TopicAndPartition, PartitionOffsetRequestInfo> infoMap = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		for (Partition partition: topicsAndPartitions) {
			infoMap.put(AS_KAFKA_TOPIC_AND_PARTITION.valueOf(partition), new PartitionOffsetRequestInfo(time, 1));
		}
		OffsetRequest offsetRequest = new OffsetRequest(infoMap, KAFKA_CONSUMER_VERSION, simpleConsumer.clientId());
		OffsetResponse offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest);
		KafkaResultBuilder<Long> kafkaResultBuilder = new KafkaResultBuilder<Long>();
		for (Partition partition : topicsAndPartitions) {
			short errorCode = offsetResponse.errorCode(partition.getTopic(), partition.getNumber());
			if (ErrorMapping.NoError() == errorCode) {
				long[] offsets = offsetResponse.offsets(partition.getTopic(), partition.getNumber());
				if (offsets.length == 0) {
					throw new IllegalStateException("No error has been returned, but no offsets either");
				}
				kafkaResultBuilder.add(partition).withResult(offsets[0]);
			}
			else {
				kafkaResultBuilder.add(partition).withError(errorCode);
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<Long> commitOffset(Partition Partition, long offset) {
		return commitOffsets(Collections.singletonMap(Partition, offset));
	}

	public KafkaResult<Long> commitOffsets(Map<Partition, Long> newOffsets) {
		Map<TopicAndPartition, OffsetMetadataAndError> requestInfo = new HashMap<TopicAndPartition, OffsetMetadataAndError>();
		for (Map.Entry<Partition, Long> newOffsetEntry : newOffsets.entrySet()) {
			requestInfo.put(
					AS_KAFKA_TOPIC_AND_PARTITION.valueOf(newOffsetEntry.getKey()),
					new OffsetMetadataAndError(newOffsetEntry.getValue(), OffsetMetadataAndError.NoMetadata(), ErrorMapping.NoError()));
		}
		OffsetCommitResponse offsetCommitResponse = simpleConsumer.commitOffsets(
				new OffsetCommitRequest(simpleConsumer.clientId(), requestInfo, KAFKA_CONSUMER_VERSION, createCorrelationId(), simpleConsumer.clientId()));
		KafkaResultBuilder<Long> kafkaResultBuilder = new KafkaResultBuilder<Long>();
		for (Partition Partition : newOffsets.keySet()) {
			if (offsetCommitResponse.errors().containsKey(Partition)) {
				kafkaResultBuilder.add(Partition).withError((Short) offsetCommitResponse.errors().get(Partition));
			}
			else {
				kafkaResultBuilder.add(Partition).withResult(newOffsets.get(Partition));
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<KafkaBrokerAddress> findLeader(String topic) {
		return findLeaders(Collections.singletonList(topic));
	}

	public KafkaResult<KafkaBrokerAddress> findLeaders(List<String> topics) {
		TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(new ArrayList<String>(topics), createCorrelationId());
		TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);
		KafkaResultBuilder<KafkaBrokerAddress> kafkaResultBuilder = new KafkaResultBuilder<KafkaBrokerAddress>();
		for (TopicMetadata topicMetadata : topicMetadataResponse.topicsMetadata()) {
			if (topicMetadata.errorCode() != ErrorMapping.NoError()) {
				kafkaResultBuilder.add(new Partition(topicMetadata.topic(), -1)).withError(topicMetadata.errorCode());
			}
			else {
				for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
					if (ErrorMapping.NoError() == partitionMetadata.errorCode()) {
						kafkaResultBuilder.add(new Partition(topicMetadata.topic(), partitionMetadata.partitionId())).withResult(new KafkaBrokerAddress(partitionMetadata.leader().host(), partitionMetadata.leader().port()));
					} else {
						kafkaResultBuilder.add(new Partition(topicMetadata.topic(), partitionMetadata.partitionId())).withError(partitionMetadata.errorCode());
					}
				}

			}
		}
		return kafkaResultBuilder.build();
	}

	/**
	 * Creates a pseudo-unique correlation id for the requests and responses
	 *
	 * @return
	 */
	private static Integer createCorrelationId() {
		return correlationIdCounter.incrementAndGet();
	}

}
