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

	public KafkaResult<MessageSet> fetch(FetchTarget fetchTarget) {
		return fetch(Collections.singletonList(fetchTarget));
	}

	public KafkaResult<MessageSet> fetch(List<FetchTarget> fetchTargets) {
		FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder();
		for (FetchTarget fetchTarget : fetchTargets) {
			fetchRequestBuilder.addFetch(fetchTarget.getTopicAndPartition().topic(), fetchTarget.getTopicAndPartition().partition(), fetchTarget.getOffset(), this.simpleConsumer.bufferSize());
		}
		FetchResponse fetchResponse = this.simpleConsumer.fetch(fetchRequestBuilder.build());
		KafkaResultBuilder<MessageSet> kafkaResultBuilder = new KafkaResultBuilder<MessageSet>();
		for (FetchTarget fetchTarget : fetchTargets) {
			short errorCode = fetchResponse.errorCode(fetchTarget.getTopicAndPartition().topic(), fetchTarget.getTopicAndPartition().partition());
			if (ErrorMapping.NoError() == errorCode) {
				kafkaResultBuilder.add(fetchTarget.getTopicAndPartition()).withResult(fetchResponse.messageSet(fetchTarget.getTopicAndPartition().topic(), fetchTarget.getTopicAndPartition().partition()));
			}
			else {
				kafkaResultBuilder.add(fetchTarget.getTopicAndPartition()).withError(fetchResponse.errorCode(fetchTarget.getTopicAndPartition().topic(), fetchTarget.getTopicAndPartition().partition()));
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<Long> fetchOffsetforConsumer(TopicAndPartition topicAndPartition) {
		return fetchOffsetForConsumer(Collections.singletonList(topicAndPartition));
	}

	public KafkaResult<Long> fetchOffsetForConsumer(List<TopicAndPartition> topicsAndPartitions) {
		OffsetFetchRequest offsetFetchRequest = new OffsetFetchRequest(simpleConsumer.clientId(), topicsAndPartitions, KAFKA_CONSUMER_VERSION, createCorrelationId(), simpleConsumer.clientId());
		OffsetFetchResponse offsetFetchResponse = simpleConsumer.fetchOffsets(offsetFetchRequest);
		KafkaResultBuilder<Long> kafkaResultBuilder = new KafkaResultBuilder<Long>();
		for (TopicAndPartition topicAndPartition : topicsAndPartitions) {
			OffsetMetadataAndError offsetMetadataAndError = offsetFetchResponse.offsets().get(topicAndPartition);
			short errorCode = offsetMetadataAndError.error();
			if (ErrorMapping.NoError() == errorCode) {
				kafkaResultBuilder.add(topicAndPartition).withResult(offsetMetadataAndError.offset());
			}
			else {
				kafkaResultBuilder.add(topicAndPartition).withError(offsetMetadataAndError.error());
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<Long> fetchInitialOffset(TopicAndPartition topicAndPartition, long time) {
		return fetchInitialOffset(Collections.singletonList(topicAndPartition), time);
	}

	public KafkaResult<Long> fetchInitialOffset(List<TopicAndPartition> topicsAndPartitions, long time) {
		Map<TopicAndPartition, PartitionOffsetRequestInfo> infoMap = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		for (TopicAndPartition topicAndPartition: topicsAndPartitions) {
			infoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(time, 1));
		}
		OffsetRequest offsetRequest = new OffsetRequest(infoMap, KAFKA_CONSUMER_VERSION, simpleConsumer.clientId());
		OffsetResponse offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest);
		KafkaResultBuilder<Long> kafkaResultBuilder = new KafkaResultBuilder<Long>();
		for (TopicAndPartition topicAndPartition : topicsAndPartitions) {
			short errorCode = offsetResponse.errorCode(topicAndPartition.topic(), topicAndPartition.partition());
			if (ErrorMapping.NoError() == errorCode) {
				long[] offsets = offsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition());
				if (offsets.length == 0) {
					throw new IllegalStateException("No error has been returned, but no offsets either");
				}
				kafkaResultBuilder.add(topicAndPartition).withResult(offsets[0]);
			}
			else {
				kafkaResultBuilder.add(topicAndPartition).withError(errorCode);
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<Long> commitOffset(TopicAndPartition topicAndPartition, long offset) {
		return commitOffsets(Collections.singletonMap(topicAndPartition, offset));
	}

	public KafkaResult<Long> commitOffsets(Map<TopicAndPartition, Long> newOffsets) {
		Map<TopicAndPartition, OffsetMetadataAndError> requestInfo = new HashMap<TopicAndPartition, OffsetMetadataAndError>();
		for (Map.Entry<TopicAndPartition, Long> newOffsetEntry : newOffsets.entrySet()) {
			requestInfo.put(
					newOffsetEntry.getKey(),
					new OffsetMetadataAndError(newOffsetEntry.getValue(), OffsetMetadataAndError.NoMetadata(), ErrorMapping.NoError()));
		}
		OffsetCommitResponse offsetCommitResponse = simpleConsumer.commitOffsets(
				new OffsetCommitRequest(simpleConsumer.clientId(), requestInfo, KAFKA_CONSUMER_VERSION, createCorrelationId(), simpleConsumer.clientId()));
		KafkaResultBuilder<Long> kafkaResultBuilder = new KafkaResultBuilder<Long>();
		for (TopicAndPartition topicAndPartition : newOffsets.keySet()) {
			if (offsetCommitResponse.errors().containsKey(topicAndPartition)) {
				kafkaResultBuilder.add(topicAndPartition).withError((Short) offsetCommitResponse.errors().get(topicAndPartition));
			}
			else {
				kafkaResultBuilder.add(topicAndPartition).withResult(newOffsets.get(topicAndPartition));
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
				kafkaResultBuilder.add(new TopicAndPartition(topicMetadata.topic(), -1)).withError(topicMetadata.errorCode());
			}
			else {
				for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
					if (ErrorMapping.NoError() == partitionMetadata.errorCode()) {
						kafkaResultBuilder.add(new TopicAndPartition(topicMetadata.topic(), partitionMetadata.partitionId())).withResult(new KafkaBrokerAddress(partitionMetadata.leader().host(), partitionMetadata.leader().port()));
					} else {
						kafkaResultBuilder.add(new TopicAndPartition(topicMetadata.topic(), partitionMetadata.partitionId())).withError(partitionMetadata.errorCode());
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
