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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.utils.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import scala.Option;
import scala.collection.JavaConversions;

import org.springframework.integration.kafka.simple.connection.KafkaBrokerAddress;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnection;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.simple.connection.KafkaConfiguration;
import org.springframework.integration.kafka.simple.connection.KafkaResult;
import org.springframework.integration.kafka.simple.connection.Partition;

/**
 * @author Marius Bogoevici
 */
public class TestKafkaConnectionFactory {

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
	public void testCreateConnectionFactory() throws Exception {
		List<KafkaBrokerAddress> brokerAddresses = Collections.singletonList(kafkaRule.getBrokerAddress());
		Partition partition = new Partition(TEST_TOPIC, 0);
		List<Partition> partitions = Collections.singletonList(partition);
		KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory = new KafkaBrokerConnectionFactory(new KafkaConfiguration(brokerAddresses));
		kafkaBrokerConnectionFactory.afterPropertiesSet();
		KafkaBrokerConnection connection = kafkaBrokerConnectionFactory.createConnection(kafkaRule.getBrokerAddress());
		KafkaResult<KafkaBrokerAddress> leaders = connection.findLeaders(TEST_TOPIC);
		assertThat(leaders.getErrors().entrySet(), empty());
		assertThat(leaders.getResult().entrySet(), hasSize(1));
		assertThat(leaders.getResult().get(partition), equalTo(kafkaRule.getBrokerAddress()));
	}
}
