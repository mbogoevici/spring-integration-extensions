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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.gs.collections.impl.list.mutable.FastList;
import junit.framework.Assert;
import kafka.admin.AdminUtils;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.utils.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import scala.Option;
import scala.collection.JavaConversions;

import org.springframework.integration.kafka.serializer.common.StringDecoder;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnection;
import org.springframework.integration.kafka.simple.connection.KafkaResult;
import org.springframework.integration.kafka.simple.connection.Partition;
import org.springframework.integration.kafka.simple.consumer.KafkaMessage;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageBatch;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageFetchRequest;
import org.springframework.integration.kafka.simple.util.MessageUtils;

/**
 * @author Marius Bogoevici
 */
public class TestKafkaBrokerConnection {

	public static final String TEST_TOPIC = "test-topic";

	@ClassRule
	public static KafkaSingleBrokerRule kafkaRule = new KafkaSingleBrokerRule();

	@BeforeClass
	public static void setUp() throws Exception {
		AdminUtils.createTopic(kafkaRule.getZookeeperClient(), TEST_TOPIC, 1, 1, new Properties());
		TestUtils.waitUntilMetadataIsPropagated(JavaConversions.asScalaBuffer(Collections.singletonList(kafkaRule.getKafkaServer())), TEST_TOPIC, 0, 5000L);
		TestUtils.waitUntilLeaderIsElectedOrChanged(kafkaRule.getZookeeperClient(), TEST_TOPIC, 0, 5000L, Option.empty());
	}

	@Test
	public void testFetchPartitionMetadata() throws Exception {
		KafkaBrokerConnection brokerConnection = new KafkaBrokerConnection(kafkaRule.getBrokerAddress());
		Partition partition = new Partition(TEST_TOPIC, 0);
		KafkaResult<Long> result = brokerConnection.fetchInitialOffset(-1, partition);
		Assert.assertEquals(0, result.getErrors().size());
		Assert.assertEquals(1, result.getResult().size());
		Assert.assertEquals(Long.valueOf(0), result.getResult().get(partition));
	}

	@Test
	public void testReceiveMessages() throws Exception {
		Producer<String, String> producer = KafkaSingleBrokerRule.createStringProducer();
		List<KeyedMessage<String, String>> keyedMessages = createMessages(10);
		producer.send(JavaConversions.asScalaBuffer(keyedMessages).toSeq());
		KafkaBrokerConnection brokerConnection = new KafkaBrokerConnection(kafkaRule.getBrokerAddress());
		Partition partition = new Partition(TEST_TOPIC, 0);
		KafkaMessageFetchRequest kafkaMessageFetchRequest = new KafkaMessageFetchRequest(partition, 0L, 1000);
		KafkaResult<KafkaMessageBatch> result = brokerConnection.fetch(kafkaMessageFetchRequest);
		Assert.assertEquals(0, result.getErrors().size());
		Assert.assertEquals(1, result.getResult().size());
		Assert.assertEquals(10, result.getResult().get(partition).getMessages().size());
		Assert.assertEquals(10,result.getResult().get(partition).getHighWatermark());
		StringDecoder decoder = new StringDecoder();
		int i = 0;
		for (KafkaMessage kafkaMessage : result.getResult().get(partition).getMessages()) {
			Assert.assertEquals("Key " + i, MessageUtils.decodeKey(kafkaMessage, decoder));
			Assert.assertEquals("Message " + i, MessageUtils.decodePayload(kafkaMessage, decoder));
			i++;
		}
	}

	public List<KeyedMessage<String, String>> createMessages(int count) {
		List<KeyedMessage<String,String>> messages = new ArrayList<KeyedMessage<String, String>>();
		for (int i=0; i<count; i++) {
			messages.add(new KeyedMessage<String, String>(TEST_TOPIC, "Key " + i, "Message " + i));
		}
		return messages;
	}

}
