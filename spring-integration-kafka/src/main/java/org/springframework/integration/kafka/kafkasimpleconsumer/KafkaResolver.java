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
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.multimap.list.FastListMultimap;
/**
 * @author Marius Bogoevici
 */
public class KafkaResolver {

	public final KafkaBrokerInstantiator KAFKA_BROKER_INSTANTIATOR = new KafkaBrokerInstantiator();


	private List<KafkaBrokerAddress> kafkaBrokerAddresses;

	private Multimap<KafkaBrokerAddress, Partition> topicsAndPartitionsByBroker = FastListMultimap.newMultimap();

	private UnifiedMap<KafkaBrokerAddress, KafkaBrokerConnection> kafkaBrokersCache = UnifiedMap.newMap();

	private UnifiedMap<Partition, KafkaBrokerAddress> brokersByPartition = UnifiedMap.newMap();

	private KafkaBrokerConnection adminBroker;

	public KafkaResolver(List<KafkaBrokerAddress> kafkaBrokerAddresses) {
		this.kafkaBrokerAddresses = FastList.newList(kafkaBrokerAddresses);
		refresh();
	}

	public KafkaBrokerConnection getAdminBroker() {
		return adminBroker;
	}

	/**
	 * Resolves the broker associated with a specific topic and partition. Internally,
	 * caches the {@link KafkaBrokerConnection}
	 *
	 * @param Partition
	 * @return the broker associated with the provided topic and partition
	 */
	public KafkaBrokerConnection resolveBroker(Partition Partition) {
		KafkaBrokerAddress kafkaBrokerAddress = brokersByPartition.get(Partition);
		return resolveAddress(kafkaBrokerAddress);
	}


	/**
	 * Resolves the broker associated with a specific topic and partition. Internally,
	 * caches the {@link KafkaBrokerConnection}
	 *
	 * @param topicsAndPartitions
	 * @return the broker associated with the provided topic and partition
	 */
	public Map<Partition, KafkaBrokerConnection> resolveBrokers(final List<Partition> topicsAndPartitions) {
		return FastList.newList(topicsAndPartitions).toMap(Functions.<Partition>getPassThru(), new Function<Partition, KafkaBrokerConnection>() {
			@Override
			public KafkaBrokerConnection valueOf(Partition partition) {
				return resolveBroker(partition);
			}
		});
	}

	public List<Partition> resolveTopicsAndPartitions(KafkaBrokerAddress kafkaBrokerAddress) {
		return topicsAndPartitionsByBroker.get(kafkaBrokerAddress).toList();
	}

	public KafkaBrokerConnection resolveAddress(KafkaBrokerAddress kafkaBrokerAddress) {
		return kafkaBrokersCache.getIfAbsentPutWith(kafkaBrokerAddress, KAFKA_BROKER_INSTANTIATOR, kafkaBrokerAddress);
	}

	public void refresh(String ... topics) {
		for (KafkaBrokerAddress kafkaBrokerAddress : kafkaBrokerAddresses) {
			KafkaBrokerConnection kafkaBrokerConnection = new KafkaBrokerConnection(kafkaBrokerAddress);
			KafkaResult<KafkaBrokerAddress> leaders = kafkaBrokerConnection.findLeaders(topics);
			if (leaders.getErrors().size() == 0) {
				this.adminBroker = kafkaBrokerConnection;
				brokersByPartition = UnifiedMap.newMap(leaders.getResult());
			}
			topicsAndPartitionsByBroker = brokersByPartition.flip();
		}
	}

	public void refresh(boolean clear, String... topics) {
		for (KafkaBrokerAddress kafkaBrokerAddress : kafkaBrokerAddresses) {
			KafkaBrokerConnection kafkaBrokerConnection = new KafkaBrokerConnection(kafkaBrokerAddress);
			KafkaResult<KafkaBrokerAddress> leaders = kafkaBrokerConnection.findLeaders(topics);
			if (leaders.getErrors().size() == 0) {
				this.adminBroker = kafkaBrokerConnection;
				brokersByPartition = UnifiedMap.newMap(leaders.getResult());
			}
			topicsAndPartitionsByBroker = brokersByPartition.flip();
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
