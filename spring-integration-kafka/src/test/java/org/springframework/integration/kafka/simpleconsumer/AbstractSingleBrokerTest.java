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
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.serializer.StringEncoder;
import kafka.utils.TestUtils;
import kafka.utils.VerifiableProperties;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.List$;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.simple.connection.KafkaConfiguration;

/**
 * @author Marius Bogoevici
 */
public class AbstractSingleBrokerTest {

	public static final String TEST_TOPIC = "test-topic";

	@ClassRule
	public static KafkaSingleBrokerRule kafkaRule = new KafkaSingleBrokerRule();

	@BeforeClass
	public static void setUp() throws Exception {
		AdminUtils.createTopic(kafkaRule.getZookeeperClient(), TEST_TOPIC, 5, 1, new Properties());
		TestUtils.waitUntilMetadataIsPropagated(JavaConversions.asScalaBuffer(Collections.singletonList(kafkaRule.getKafkaServer())), TEST_TOPIC, 0, 5000L);
//		TestUtils.waitUntilLeaderIsElectedOrChanged(kafkaRule.getZookeeperClient(), TEST_TOPIC, 0, 5000L, Option.empty());
//		TestUtils.waitUntilLeaderIsElectedOrChanged(kafkaRule.getZookeeperClient(), TEST_TOPIC, 1, 5000L, Option.empty());
//		TestUtils.waitUntilLeaderIsElectedOrChanged(kafkaRule.getZookeeperClient(), TEST_TOPIC, 2, 5000L, Option.empty());
//		TestUtils.waitUntilLeaderIsElectedOrChanged(kafkaRule.getZookeeperClient(), TEST_TOPIC, 3, 5000L, Option.empty());
//		TestUtils.waitUntilLeaderIsElectedOrChanged(kafkaRule.getZookeeperClient(), TEST_TOPIC, 4, 5000L, Option.empty());
	}

	public KafkaConfiguration getKafkaConfiguration() {
		return new KafkaConfiguration(Collections.singletonList(kafkaRule.getBrokerAddress()));
	}

	public List<KeyedMessage<String, String>> createMessages(int count) {
		List<KeyedMessage<String,String>> messages = new ArrayList<KeyedMessage<String, String>>();
		for (int i=0; i<count; i++) {
			messages.add(new KeyedMessage<String, String>(TEST_TOPIC, "Key " + i, "Message " + i));
		}
		return messages;
	}

	public Producer<String, String> createStringProducer() {
		StringEncoder encoder = new StringEncoder(new VerifiableProperties());
		return TestUtils.createProducer(kafkaRule.getKafkaServer().config().hostName() + ":" + kafkaRule.getKafkaServer().config().port(), encoder, encoder);
	}

	public KafkaBrokerConnectionFactory getKafkaBrokerConnectionFactory() throws Exception {
		KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory = new KafkaBrokerConnectionFactory(getKafkaConfiguration());
		kafkaBrokerConnectionFactory.afterPropertiesSet();
		return kafkaBrokerConnectionFactory;
	}
}
