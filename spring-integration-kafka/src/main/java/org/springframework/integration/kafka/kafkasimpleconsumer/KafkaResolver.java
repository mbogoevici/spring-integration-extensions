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

import java.util.List;
import java.util.Map;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.multimap.Multimap;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.multimap.list.FastListMultimap;
import kafka.common.TopicAndPartition;

/**
 * @author Marius Bogoevici
 */
public class KafkaResolver {

	public final KafkaBrokerInstantiator KAFKA_BROKER_INSTANTIATOR = new KafkaBrokerInstantiator();

	public static final Function<TopicAndPartition, TopicAndPartition> IDENTITY = new Function<TopicAndPartition, TopicAndPartition>() {
		@Override
		public TopicAndPartition valueOf(TopicAndPartition object) {
			return object;
		}
	};

	private KafkaConfiguration configuration;

	private Multimap<KafkaBrokerAddress, TopicAndPartition> topicsAndPartitionsByBroker = FastListMultimap.newMultimap();

	private UnifiedMap<KafkaBrokerAddress, KafkaBrokerConnection> kafkaBrokersCache = UnifiedMap.newMap();

	private UnifiedMap<TopicAndPartition, KafkaBrokerAddress> brokersByTopicAndPartition = UnifiedMap.newMap();

	private KafkaBrokerConnection adminBroker;

	public KafkaResolver(KafkaConfiguration configuration) {
		this.configuration = configuration;
		refresh();
	}

	public KafkaBrokerConnection getAdminBroker() {
		return adminBroker;
	}

	/**
	 * Resolves the broker associated with a specific topic and partition. Internally,
	 * caches the {@link KafkaBrokerConnection}
	 *
	 * @param topicAndPartition
	 * @return the broker associated with the provided topic and partition
	 */
	public KafkaBrokerConnection resolveBroker(TopicAndPartition topicAndPartition) {
		KafkaBrokerAddress kafkaBrokerAddress = brokersByTopicAndPartition.get(topicAndPartition);
		return resolveAddress(kafkaBrokerAddress);
	}


	/**
	 * Resolves the broker associated with a specific topic and partition. Internally,
	 * caches the {@link KafkaBrokerConnection}
	 *
	 * @param topicsAndPartitions
	 * @return the broker associated with the provided topic and partition
	 */
	public Map<TopicAndPartition, KafkaBrokerConnection> resolveBrokers(final List<TopicAndPartition> topicsAndPartitions) {
		return FastList.newList(topicsAndPartitions).toMap(IDENTITY, new Function<TopicAndPartition, KafkaBrokerConnection>() {
			@Override
			public KafkaBrokerConnection valueOf(TopicAndPartition object) {
				return resolveBroker(object);
			}
		});
	}

	public List<TopicAndPartition> resolveTopicsAndPartitions(KafkaBrokerAddress kafkaBrokerAddress) {
		return topicsAndPartitionsByBroker.get(kafkaBrokerAddress).toList();
	}

	public KafkaBrokerConnection resolveAddress(KafkaBrokerAddress kafkaBrokerAddress) {
		return kafkaBrokersCache.getIfAbsentPutWith(kafkaBrokerAddress, KAFKA_BROKER_INSTANTIATOR, kafkaBrokerAddress);
	}

	public void refresh() {
		for (KafkaBrokerAddress kafkaBrokerAddress : configuration.getBrokerAddresses()) {
			KafkaBrokerConnection kafkaBrokerConnection = new KafkaBrokerConnection(kafkaBrokerAddress);
			KafkaResult<KafkaBrokerAddress> leaders = kafkaBrokerConnection.findLeaders(configuration.getTopics());
			if (leaders.getErrors().size() == 0) {
				this.adminBroker = kafkaBrokerConnection;
				brokersByTopicAndPartition = UnifiedMap.newMap(leaders.getResult());
			}
			topicsAndPartitionsByBroker = brokersByTopicAndPartition.flip();
		}
	}


	private class KafkaBrokerInstantiator implements Function<KafkaBrokerAddress, KafkaBrokerConnection> {
		@Override
		public KafkaBrokerConnection valueOf(KafkaBrokerAddress kafkaBrokerAddress) {
			if (KafkaResolver.this.adminBroker.getBrokerAddress().equals(kafkaBrokerAddress))
				return KafkaResolver.this.adminBroker;
			else
				return new KafkaBrokerConnection(kafkaBrokerAddress);
		}
	}
}
