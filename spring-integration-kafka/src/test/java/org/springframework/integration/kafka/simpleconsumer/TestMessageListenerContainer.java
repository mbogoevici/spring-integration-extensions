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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gs.collections.api.multimap.MutableMultimap;
import com.gs.collections.impl.factory.Multimaps;
import com.gs.collections.impl.list.mutable.FastList;
import kafka.utils.VerifiableProperties;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.collection.JavaConversions;

import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.simple.consumer.KafkaMessage;
import org.springframework.integration.kafka.simple.listener.KafkaMessageListenerContainer;
import org.springframework.integration.kafka.simple.listener.MessageListener;
import org.springframework.integration.kafka.simple.offset.MetadataStoreOffsetManager;
import org.springframework.integration.kafka.simple.util.MessageUtils;

/**
 * @author Marius Bogoevici
 */
public class TestMessageListenerContainer extends AbstractSingleBrokerTest {

	@BeforeClass
	public static void setUp() throws Exception {
		MutableMultimap<Integer, Integer> partitionDistribution = Multimaps.mutable.list.with();
		partitionDistribution.put(0,0);
		partitionDistribution.put(1,0);
		partitionDistribution.put(2,0);
		partitionDistribution.put(3,0);
		partitionDistribution.put(4,0);
		createTopic(TEST_TOPIC, partitionDistribution);
	}

	@Test
	public void testMessageListenerContainerOnTopic() throws Exception {

		KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory = getKafkaBrokerConnectionFactory();

		MetadataStoreOffsetManager offsetManager = new MetadataStoreOffsetManager(kafkaBrokerConnectionFactory);
		offsetManager.afterPropertiesSet();
		KafkaMessageListenerContainer kafkaMessageListenerContainer = new KafkaMessageListenerContainer(kafkaBrokerConnectionFactory, offsetManager, new String[]{TEST_TOPIC});
		kafkaMessageListenerContainer.setConcurrency(2);
		final ArrayList<FastList<Object>> receivedData = new ArrayList<FastList<Object>>();

		final CountDownLatch latch = new CountDownLatch(10);
		kafkaMessageListenerContainer.setMessageListener(new MessageListener() {
			@Override
			public void onMessage(KafkaMessage message) {
				kafka.serializer.StringDecoder decoder = new kafka.serializer.StringDecoder(new VerifiableProperties());
				receivedData.add(FastList.<Object>newListWith(message.getPartition(), message.getOffset(),MessageUtils.decodeKey(message, decoder), MessageUtils.decodePayload(message, decoder)));
				latch.countDown();
			}
		});

		kafkaMessageListenerContainer.afterPropertiesSet();
		kafkaMessageListenerContainer.start();

		createStringProducer().send(JavaConversions.asScalaBuffer(createMessages(10)));
		latch.await(1, TimeUnit.MINUTES);

		kafkaMessageListenerContainer.stop();

	}

}
