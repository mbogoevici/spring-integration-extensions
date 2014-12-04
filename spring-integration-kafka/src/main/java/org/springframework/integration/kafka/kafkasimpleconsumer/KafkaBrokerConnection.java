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

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
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

import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 */
public class KafkaBrokerConnection {

	public static final String DEFAULT_CLIENT_ID = "spring.integration.kafka";

	public static final int DEFAULT_BUFFER_SIZE = 400000000;

	public static final int DEFAULT_SOCKET_TIMEOUT = 10000;

	private final SimpleConsumer simpleConsumer;

	private final static AtomicInteger correlationIdCounter = new AtomicInteger(new Random(new Date().getTime()).nextInt());

	private KafkaBrokerAddress brokerAddress;

	public KafkaBrokerConnection(KafkaBrokerAddress brokerAddress) {
		this(brokerAddress, DEFAULT_CLIENT_ID);
	}

	public KafkaBrokerConnection(KafkaBrokerAddress brokerAddress, String clientId) {
		this(brokerAddress, clientId,DEFAULT_BUFFER_SIZE,DEFAULT_SOCKET_TIMEOUT);
	}

	public KafkaBrokerConnection(KafkaBrokerAddress brokerAddress, String clientId, int bufferSize, int soTimeout) {
		this.brokerAddress = brokerAddress;
		this.simpleConsumer = new SimpleConsumer(brokerAddress.getHost(), brokerAddress.getPort(), soTimeout, bufferSize, clientId);
	}

	public KafkaBrokerAddress getBrokerAddress() {
		return brokerAddress;
	}

	public KafkaResult<KafkaMessageSet> fetch(KafkaMessageFetchRequest... kafkaMessageFetchRequests) {
		FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder();
		for (KafkaMessageFetchRequest kafkaMessageFetchRequest : kafkaMessageFetchRequests) {
			fetchRequestBuilder.addFetch(kafkaMessageFetchRequest.getPartition().getTopic(), kafkaMessageFetchRequest.getPartition().getNumber(), kafkaMessageFetchRequest.getOffset(), kafkaMessageFetchRequest.getMaxSize());
		}
		FetchResponse fetchResponse = this.simpleConsumer.fetch(fetchRequestBuilder.build());
		KafkaResultBuilder<KafkaMessageSet> kafkaResultBuilder = new KafkaResultBuilder<KafkaMessageSet>();
		for (KafkaMessageFetchRequest kafkaMessageFetchRequest : kafkaMessageFetchRequests) {
			System.out.println("Reading from " + kafkaMessageFetchRequest.getPartition() + "@" + kafkaMessageFetchRequest.getOffset());
			short errorCode = fetchResponse.errorCode(kafkaMessageFetchRequest.getPartition().getTopic(), kafkaMessageFetchRequest.getPartition().getNumber());
			if (ErrorMapping.NoError() == errorCode) {
				kafkaResultBuilder.add(kafkaMessageFetchRequest.getPartition()).withResult(new KafkaMessageSet(fetchResponse.messageSet(kafkaMessageFetchRequest.getPartition().getTopic(), kafkaMessageFetchRequest.getPartition().getNumber()),fetchResponse.highWatermark(kafkaMessageFetchRequest.getPartition().getTopic(), kafkaMessageFetchRequest.getPartition().getNumber())));
			}
			else {
				kafkaResultBuilder.add(kafkaMessageFetchRequest.getPartition()).withError(fetchResponse.errorCode(kafkaMessageFetchRequest.getPartition().getTopic(), kafkaMessageFetchRequest.getPartition().getNumber()));
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<Long> fetchStoredOffsetsForConsumer(String consumerId, Partition... partitions) {
		FastList<TopicAndPartition> topicsAndPartitions = FastList.newList(Arrays.asList(partitions)).collect(new Function<Partition, TopicAndPartition>() {
			@Override
			public TopicAndPartition valueOf(Partition partition) {
				return new TopicAndPartition(partition.getTopic(), partition.getNumber());
			}
		});
		OffsetFetchRequest offsetFetchRequest = new OffsetFetchRequest(consumerId, topicsAndPartitions, kafka.api.OffsetFetchRequest.CurrentVersion(), createCorrelationId(), simpleConsumer.clientId());
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

	public KafkaResult<Long> fetchInitialOffset(long referenceTime, Partition... topicsAndPartitions) {
		Assert.isTrue(topicsAndPartitions.length > 0, "Must provide at least one partition");
		Map<TopicAndPartition, PartitionOffsetRequestInfo> infoMap = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		for (Partition partition: topicsAndPartitions) {
			infoMap.put(new TopicAndPartition(partition.getTopic(), partition.getNumber()), new PartitionOffsetRequestInfo(referenceTime, 1));
		}
		OffsetRequest offsetRequest = new OffsetRequest(infoMap, kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());
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

	public KafkaResult<Void> commitOffsets(Offset... offsets) {
		Map<TopicAndPartition, OffsetMetadataAndError> requestInfo = new HashMap<TopicAndPartition, OffsetMetadataAndError>();
		for (Offset offset : offsets) {
			Partition partition = offset.getPartition();
			requestInfo.put(
					new TopicAndPartition(partition.getTopic(), partition.getNumber()),
					new OffsetMetadataAndError(offset.getOffset(), OffsetMetadataAndError.NoMetadata(), ErrorMapping.NoError()));
		}
		OffsetCommitResponse offsetCommitResponse = simpleConsumer.commitOffsets(
				new OffsetCommitRequest(simpleConsumer.clientId(), requestInfo, kafka.api.OffsetCommitRequest.CurrentVersion(), createCorrelationId(), simpleConsumer.clientId()));
		KafkaResultBuilder<Void> kafkaResultBuilder = new KafkaResultBuilder<Void>();
		for (TopicAndPartition topicAndPartition : requestInfo.keySet()) {
			if (offsetCommitResponse.errors().containsKey(topicAndPartition)) {
				kafkaResultBuilder.add(new Partition(topicAndPartition.topic(), topicAndPartition.partition())).withError((Short) offsetCommitResponse.errors().get(topicAndPartition));
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<KafkaBrokerAddress> findLeaders(String... topics) {
		TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Arrays.asList(topics), createCorrelationId());
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
