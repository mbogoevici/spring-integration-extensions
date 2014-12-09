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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;

import com.gs.collections.impl.list.mutable.FastList;
import junit.framework.Assert;
import kafka.admin.AdminUtils;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.serializer.StringEncoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import scala.Option;
import scala.collection.JavaConversions;

import org.springframework.integration.kafka.serializer.common.StringDecoder;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerAddress;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnection;
import org.springframework.integration.kafka.simple.connection.KafkaResult;
import org.springframework.integration.kafka.simple.connection.Partition;
import org.springframework.integration.kafka.simple.consumer.KafkaMessage;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageBatch;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageFetchRequest;

/**
 * @author Marius Bogoevici
 */
public class TestKafka {

	public static final String TEST_TOPIC = "test-topic";

	private static KafkaServer server;

	private static EmbeddedZookeeper zookeeper;

	private static ZkClient zkClient;

	@ClassRule
	public static ExternalResource kafkaServer = new ExternalResource() {

		private int kafkaPort;

		@Override
		protected void before() throws Throwable {
			zookeeper = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
			int zkConnectionTimeout = 6000;
			int zkSessionTimeout = 6000;
			kafkaPort = TestUtils.choosePort();
			Properties brokerConfig = TestUtils.createBrokerConfig(0, kafkaPort);
			KafkaConfig kafkaConfig = new KafkaConfig(brokerConfig);
			zkClient = new ZkClient(TestZKUtils.zookeeperConnect(), zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer$.MODULE$);
			server = TestUtils.createServer(kafkaConfig, SystemTime$.MODULE$);
		}

		@Override
		protected void after() {
			server.shutdown();
			Utils.rm(server.config().logDirs());
			zkClient.close();
			zookeeper.shutdown();
		}
	};


	@BeforeClass
	public static void setUp() throws Exception {
		AdminUtils.createTopic(zkClient, TEST_TOPIC, 1, 1, new Properties());
		TestUtils.waitUntilMetadataIsPropagated(JavaConversions.asScalaBuffer(Collections.singletonList(server)), TEST_TOPIC, 0, 5000L);
		TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, TEST_TOPIC, 0, 5000L, Option.empty());
	}

	@Test
	public void testFetchPartitionMetadata() throws Exception {
		KafkaBrokerConnection brokerConnection = new KafkaBrokerConnection(new KafkaBrokerAddress(server.config().hostName(), server.config().port()));
		KafkaResult<Long> result = brokerConnection.fetchInitialOffset(-1, new Partition(TEST_TOPIC, 0));
		Assert.assertEquals(0, result.getErrors().size());
		Assert.assertEquals(1, result.getResult().size());
		Assert.assertEquals(0L,result.getResult().get(new Partition(TEST_TOPIC, 0)).longValue());

	}

	@Test
	public void testReceiveMessages() throws Exception {
		StringEncoder encoder = new StringEncoder(new VerifiableProperties());
		StringDecoder decoder = new StringDecoder();
		Producer<String, String> producer = TestUtils.createProducer(server.config().hostName() + ":" + server.config().port(), encoder, encoder);
		FastList<KeyedMessage<String, String>> keyedMessages = FastList.newListWith(new KeyedMessage<String, String>(TEST_TOPIC, "1", "Message 1"));
		producer.send(JavaConversions.asScalaBuffer(keyedMessages).toSeq());
		KafkaBrokerConnection brokerConnection = new KafkaBrokerConnection(new KafkaBrokerAddress(server.config().hostName(), server.config().port()));
		Partition key = new Partition(TEST_TOPIC, 0);
		KafkaMessageFetchRequest kafkaMessageFetchRequest = new KafkaMessageFetchRequest(key, 0L, 1000);
		KafkaResult<KafkaMessageBatch> result = brokerConnection.fetch(kafkaMessageFetchRequest);
		Assert.assertEquals(0, result.getErrors().size());
		Assert.assertEquals(1, result.getResult().size());
		Assert.assertEquals(1L,result.getResult().get(key).getHighWatermark());
		for (KafkaMessage kafkaMessage : result.getResult().get(key).getMessages()) {
			Assert.assertTrue(kafkaMessage.getMessage().hasKey());
			byte[] dst = new byte[kafkaMessage.getMessage().payloadSize()];
			kafkaMessage.getMessage().payload().get(dst);
			Assert.assertEquals("Message 1", decoder.fromBytes(dst));
		}

	}
}
