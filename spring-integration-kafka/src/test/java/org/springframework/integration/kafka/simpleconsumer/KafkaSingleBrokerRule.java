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

import java.util.Properties;

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
import org.junit.rules.ExternalResource;

import org.springframework.integration.kafka.simple.connection.KafkaBrokerAddress;

/**
* @author Marius Bogoevici
*/
public class KafkaSingleBrokerRule extends ExternalResource {

	private int kafkaPort;

	private KafkaServer kafkaServer;

	private static EmbeddedZookeeper zookeeper;

	private static ZkClient zookeeperClient;

	public static Producer<String, String> createStringProducer() {
		StringEncoder encoder = new StringEncoder(new VerifiableProperties());
		return TestUtils.createProducer(TestKafkaBrokerConnection.kafkaRule.getKafkaServer().config().hostName() + ":" + TestKafkaBrokerConnection.kafkaRule.getKafkaServer().config().port(), encoder, encoder);
	}

	@Override
	protected void before() throws Throwable {
		zookeeper = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
		int zkConnectionTimeout = 6000;
		int zkSessionTimeout = 6000;
		kafkaPort = TestUtils.choosePort();
		Properties brokerConfig = TestUtils.createBrokerConfig(0, kafkaPort);
		KafkaConfig kafkaConfig = new KafkaConfig(brokerConfig);
		zookeeperClient = new ZkClient(TestZKUtils.zookeeperConnect(), zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer$.MODULE$);
		kafkaServer = TestUtils.createServer(kafkaConfig, SystemTime$.MODULE$);
	}

	@Override
	protected void after() {
		kafkaServer.shutdown();
		Utils.rm(kafkaServer.config().logDirs());
		zookeeperClient.close();
		zookeeper.shutdown();
	}

	public int getKafkaPort() {
		return kafkaPort;
	}

	public KafkaServer getKafkaServer() {
		return kafkaServer;
	}

	public EmbeddedZookeeper getZookeeper() {
		return zookeeper;
	}

	public ZkClient getZookeeperClient() {
		return zookeeperClient;
	}

	public KafkaBrokerAddress getBrokerAddress() {
		return new KafkaBrokerAddress(getKafkaServer().config().hostName(), getKafkaServer().config().port());
	}
}
