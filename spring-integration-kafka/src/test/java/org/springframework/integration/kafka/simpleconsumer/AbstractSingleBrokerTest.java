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

import com.gs.collections.api.RichIterable;
import com.gs.collections.api.block.function.Function2;
import com.gs.collections.api.multimap.Multimap;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.impl.tuple.Tuples;
import kafka.admin.AdminUtils;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import kafka.utils.TestUtils;
import kafka.utils.VerifiableProperties;
import org.junit.ClassRule;
import scala.collection.JavaConversions;
import scala.collection.Map;
import scala.collection.immutable.List$;
import scala.collection.immutable.Map$;
import scala.collection.immutable.Seq;

import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.simple.connection.KafkaConfiguration;

/**
 * @author Marius Bogoevici
 */
public class AbstractSingleBrokerTest {

	public static final String TEST_TOPIC = "test-topic";

	@ClassRule
	public static KafkaSingleBrokerRule kafkaRule = new KafkaSingleBrokerRule();

	public static void createTopic(String topicName, Multimap<Integer, Integer> partitionDistribution) {
		AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(kafkaRule.getZookeeperClient(), topicName, toKafkaPartitionMap(partitionDistribution), AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK$default$4(), AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK$default$5());
		TestUtils.waitUntilMetadataIsPropagated(JavaConversions.asScalaBuffer(Collections.singletonList(kafkaRule.getKafkaServer())), TEST_TOPIC, 0, 5000L);
	}

	public static Map toKafkaPartitionMap(Multimap<Integer, Integer> partitions) {
		java.util.Map<Object, Seq<Object>> m = partitions.toMap().collect(new Function2<Integer, RichIterable<Integer>, Pair<Object, Seq<Object>>>() {
			@Override
			public Pair<Object, Seq<Object>> value(Integer argument1, RichIterable<Integer> argument2) {
				return Tuples.pair((Object) argument1, List$.MODULE$.fromArray(argument2.toArray(new Object[0])).toSeq());
			}
		});
		return Map$.MODULE$.apply(JavaConversions.asScalaMap(m).toSeq());
	}

	public KafkaConfiguration getKafkaConfiguration() {
		return new KafkaConfiguration(Collections.singletonList(kafkaRule.getBrokerAddress()));
	}

	public scala.collection.Seq<KeyedMessage<String, String>> createMessages(int count) {
		List<KeyedMessage<String,String>> messages = new ArrayList<KeyedMessage<String, String>>();
		for (int i=0; i<count; i++) {
			messages.add(new KeyedMessage<String, String>(TEST_TOPIC, "Key " + i, i, "Message " + i));
		}
		return JavaConversions.asScalaBuffer(messages).toSeq();
	}

	public Producer<String, String> createStringProducer() {
		StringEncoder encoder = new StringEncoder(new VerifiableProperties());
		Properties producerConfig = TestUtils.getProducerConfig(kafkaRule.getKafkaServer().config().hostName() + ":" + kafkaRule.getKafkaServer().config().port(), "org.springframework.integration.kafka.simpleconsumer.TestPartitioner");
		producerConfig.put("serializer.class", StringEncoder.class.getCanonicalName());
		producerConfig.put("key.serializer.class",  StringEncoder.class.getCanonicalName());
		return new Producer<String, String>(new ProducerConfig(producerConfig));
	}

	public KafkaBrokerConnectionFactory getKafkaBrokerConnectionFactory() throws Exception {
		KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory = new KafkaBrokerConnectionFactory(getKafkaConfiguration());
		kafkaBrokerConnectionFactory.afterPropertiesSet();
		return kafkaBrokerConnectionFactory;
	}

}
