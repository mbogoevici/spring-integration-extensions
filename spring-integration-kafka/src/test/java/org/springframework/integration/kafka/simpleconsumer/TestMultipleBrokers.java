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


package org.springframework.integration.kafka.simpleconsumer;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.integration.kafka.simple.util.MessageUtils.decodeKey;
import static org.springframework.integration.kafka.simple.util.MessageUtils.decodePayload;
import static org.springframework.integration.kafka.simpleconsumer.AbstractSingleBrokerTest.createMessages;
import static org.springframework.integration.kafka.simpleconsumer.AbstractSingleBrokerTest.toKafkaPartitionMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gs.collections.api.RichIterable;
import com.gs.collections.api.bag.MutableBag;
import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.function.Function2;
import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.api.multimap.Multimap;
import com.gs.collections.api.multimap.MutableMultimap;
import com.gs.collections.api.multimap.list.MutableListMultimap;
import com.gs.collections.api.set.MutableSet;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.impl.factory.Multimaps;
import com.gs.collections.impl.factory.Sets;
import com.gs.collections.impl.multimap.list.SynchronizedPutFastListMultimap;
import com.gs.collections.impl.tuple.Tuples;
import kafka.admin.AdminUtils;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import kafka.serializer.StringEncoder;
import kafka.utils.TestUtils;
import kafka.utils.VerifiableProperties;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import scala.collection.JavaConversions;

import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.simple.connection.KafkaConfiguration;
import org.springframework.integration.kafka.simple.connection.Partition;
import org.springframework.integration.kafka.simple.consumer.KafkaMessage;
import org.springframework.integration.kafka.simple.listener.KafkaMessageListenerContainer;
import org.springframework.integration.kafka.simple.listener.MessageListener;

/**
 * @author Marius Bogoevici
 */
public class TestMultipleBrokers {

	private static final int PARTITION_COUNT = 100;

	private static final String TEST_TOPIC = "test-topic";

	public static final int BROKER_COUNT = 2;

	@Rule
	public KafkaMultiBrokerRule kafkaRule = new KafkaMultiBrokerRule(BROKER_COUNT);

	@Before
	public void setUp() throws Exception {
		MutableMultimap<Integer, Integer> partitionDistribution = Multimaps.mutable.list.with();
		for (int i = 0; i < PARTITION_COUNT; i++) {
			partitionDistribution.put(i,i%BROKER_COUNT);
		}
		createTopic(TEST_TOPIC, partitionDistribution);
	}


	public void createTopic(String topicName, Multimap<Integer, Integer> partitionDistribution) {
		AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(kafkaRule.getZookeeperClient(), topicName, toKafkaPartitionMap(partitionDistribution), AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK$default$4(), AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK$default$5());
		TestUtils.waitUntilMetadataIsPropagated(JavaConversions.asScalaBuffer(kafkaRule.getKafkaServers()), TEST_TOPIC, 0, 5000L);
		TestUtils.waitUntilMetadataIsPropagated(JavaConversions.asScalaBuffer(kafkaRule.getKafkaServers()), TEST_TOPIC, 1, 5000L);
		try {
			Thread.currentThread().sleep(10000);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testSendWithMultipleBrokers() throws Exception {
		runMessageListenerTest(100, 10, PARTITION_COUNT, 10000);
	}

	public void runMessageListenerTest(int maxReceiveSize, int concurrency, int partitionCount, int testMessageCount) throws Exception {

		KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory = new KafkaBrokerConnectionFactory(new KafkaConfiguration(kafkaRule.getBrokerAddresses()));
		kafkaBrokerConnectionFactory.afterPropertiesSet();
		ArrayList<Partition> readPartitions = new ArrayList<Partition>();
		int divisionFactor = 1;
		for (int i = 0; i < partitionCount; i++) {
			if(i % divisionFactor == 0) {
				readPartitions.add(new Partition(TEST_TOPIC, i));
			}
		}
		final KafkaMessageListenerContainer kafkaMessageListenerContainer = new KafkaMessageListenerContainer(kafkaBrokerConnectionFactory, readPartitions.toArray(new Partition[readPartitions.size()]));
		kafkaMessageListenerContainer.setMaxSize(maxReceiveSize);
		kafkaMessageListenerContainer.setConcurrency(concurrency);

		int expectedMessageCount = testMessageCount / divisionFactor;

		final MutableListMultimap<Integer,KeyedMessageWithOffset> receivedData = new SynchronizedPutFastListMultimap<Integer, KeyedMessageWithOffset>();
		final CountDownLatch latch = new CountDownLatch(expectedMessageCount);
		kafkaMessageListenerContainer.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(KafkaMessage message) {
				StringDecoder decoder = new StringDecoder(new VerifiableProperties());
				receivedData.put(message.getPartition().getId(),new KeyedMessageWithOffset(decodeKey(message, decoder), decodePayload(message, decoder), message.getOffset(), Thread.currentThread().getName(), message.getPartition().getId()));
				latch.countDown();
			}
		});

		kafkaMessageListenerContainer.start();

		createStringProducer().send(createMessages(testMessageCount));

		latch.await((expectedMessageCount/5000) + 1, TimeUnit.MINUTES);
		kafkaMessageListenerContainer.stop();

		assertThat(receivedData.valuesView().toList(), hasSize(expectedMessageCount));
		assertThat(latch.getCount(), equalTo(0L));
		System.out.println("All messages received ... checking ");

		validateMessageReceipt(receivedData, concurrency, partitionCount, testMessageCount, expectedMessageCount, readPartitions, divisionFactor);

	}

	public void validateMessageReceipt(MutableListMultimap<Integer, KeyedMessageWithOffset> receivedData, int concurrency, int partitionCount, int testMessageCount, int expectedMessageCount, ArrayList<Partition> readPartitions, int divisionFactor) {
		// Group messages received by processing thread
		MutableListMultimap<String, KeyedMessageWithOffset> messagesByThread = receivedData.valuesView().toList().groupBy(new Function<KeyedMessageWithOffset, String>() {
			@Override
			public String valueOf(KeyedMessageWithOffset object) {
				return object.getThreadName();
			}
		});

		// Execution has taken place on as many distinct threads as configured
		assertThat(messagesByThread.keysView().size(), Matchers.equalTo(concurrency));

		// Group partitions by thread
		MutableMap<String, MutableSet<Integer>> partitionsByThread = messagesByThread.toMap().collect(new Function2<String, RichIterable<KeyedMessageWithOffset>, Pair<String, MutableSet<Integer>>>() {
			@Override
			public Pair<String, MutableSet<Integer>> value(String argument1, RichIterable<KeyedMessageWithOffset> argument2) {
				return Tuples.pair(argument1, argument2.collect(new Function<KeyedMessageWithOffset, Integer>() {
					@Override
					public Integer valueOf(KeyedMessageWithOffset object) {
						return object.getPartition();
					}
				}).toSet());
			}
		});

		// Messages from a partition have been executed on the same thread and groups are mutually exclusive
		final MutableSet<Integer> validatedPartitions = Sets.mutable.of();
		partitionsByThread.valuesView().forEach(new Procedure<MutableSet<Integer>>() {
			@Override
			public void value(MutableSet<Integer> partitions) {
				assertThat(validatedPartitions.intersect(partitions), empty());
				validatedPartitions.addAll(partitions);
			}
		});

		// All partitions are accounted for, but only the ones that we were expecting to read from
		for (int i = 0; i < partitionCount; i++) {
			if (i % divisionFactor == 0) {
				assertThat(validatedPartitions, hasItem(i));
			} else {
				assertThat(validatedPartitions, not(hasItem(i)));
			}
		}

		// Sort data by payload in order to identify duplicates
		MutableList<String> sortedPayloads = receivedData.valuesView().toList().collect(new Function<KeyedMessageWithOffset, String>() {
			@Override
			public String valueOf(KeyedMessageWithOffset object) {
				return object.getPayload();
			}
		}).sortThis();

		// Remove unique values - what is left are duplicates
		MutableBag<String> duplicates = sortedPayloads.toBag();
		duplicates.removeAll(sortedPayloads.toSet());

		// The final set has exactly the same size as the message count
		assertThat(sortedPayloads, hasSize(expectedMessageCount));

		// There are no duplicates - all messages have been received only once
		assertThat(duplicates, hasSize(0));

		// Group offsets by partition
		MutableMap<Integer, MutableList<Long>> offsetsByPartition = receivedData.toMap().collect(new Function2<Integer, RichIterable<KeyedMessageWithOffset>, Pair<Integer, MutableList<Long>>>() {
			@Override
			public Pair<Integer, MutableList<Long>> value(Integer partition, RichIterable<KeyedMessageWithOffset> argument2) {
				return Tuples.pair(partition, argument2.collect(new Function<KeyedMessageWithOffset, Long>() {
					@Override
					public Long valueOf(KeyedMessageWithOffset object) {
						return object.getOffset();
					}
				}).toList());
			}
		});

		// Check that the sequence of offsets has been processed in ascending order, with no gaps
		for (MutableList<Long> offsetsForPartition : offsetsByPartition.valuesView()) {
			for (int i = 0; i < offsetsForPartition.size() - 1; i++) {
				assertThat(offsetsForPartition.get(i+1), equalTo(offsetsForPartition.get(i) + 1));
			}
		}
	}

	public Producer<String, String> createStringProducer() {
		StringEncoder encoder = new StringEncoder(new VerifiableProperties());
		Properties producerConfig = TestUtils.getProducerConfig(kafkaRule.getBrokersAsString(), "org.springframework.integration.kafka.simpleconsumer.TestPartitioner");
		producerConfig.put("serializer.class", StringEncoder.class.getCanonicalName());
		producerConfig.put("key.serializer.class",  StringEncoder.class.getCanonicalName());
		return new Producer<String, String>(new ProducerConfig(producerConfig));
	}


	static class KeyedMessageWithOffset {

		String key;

		String payload;

		Long offset;

		String threadName;

		int partition;

		public KeyedMessageWithOffset(String key, String payload, Long offset, String threadName, int partition) {
			this.key = key;
			this.payload = payload;
			this.offset = offset;
			this.threadName = threadName;
			this.partition = partition;
		}

		public String getKey() {
			return key;
		}

		public String getPayload() {
			return payload;
		}

		public Long getOffset() {
			return offset;
		}

		public String getThreadName() {
			return threadName;
		}

		public int getPartition() {
			return partition;
		}
	}
}
