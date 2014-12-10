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


package org.springframework.integration.kafka.simple.connection;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;

/**
 * @author Marius Bogoevici
 */
public class KafkaBrokerConnectionFactory {

	private final String[] topics;

	private final FastList<KafkaBrokerAddress> kafkaBrokerAddresses;

	private UnifiedMap<KafkaBrokerAddress, KafkaBrokerConnection> kafkaBrokersCache = UnifiedMap.newMap();

	private final AtomicReference<PartitionBrokerMap> partitionBrokerMapReference = new AtomicReference<PartitionBrokerMap>();

	private KafkaBrokerConnection defaultBroker;

	public KafkaBrokerConnectionFactory(KafkaConfiguration kafkaConfiguration) {
		this.kafkaBrokerAddresses = FastList.newList(kafkaConfiguration.getBrokerAddresses());
		this.topics = FastList.newList(kafkaConfiguration.getPartitions()).collect(new Function<Partition, String>() {
			@Override
			public String valueOf(Partition partition) {
				return partition.getTopic();
			}
		}).distinct().toTypedArray(String.class);
		refresh();
	}

	public List<KafkaBrokerAddress> getBrokerAddresses() {
		return kafkaBrokerAddresses;
	}


	/**
	 * Resolves the broker associated with a specific topic and partition. Internally,
	 * caches the {@link KafkaBrokerConnection}
	 *
	 * @param partition
	 * @return the broker associated with the provided topic and partition
	 */
	public KafkaBrokerConnection getLeaderConnection(Partition partition) {
		KafkaBrokerAddress kafkaBrokerAddress = partitionBrokerMapReference.get().getBrokersByPartition().get(partition);
		return createConnection(kafkaBrokerAddress);
	}


	/**
	 * Resolves the broker associated with a specific topic and partition. Internally,
	 * caches the {@link KafkaBrokerConnection}
	 *
	 * @param topicsAndPartitions
	 * @return the broker associated with the provided topic and partition
	 */
	public Map<Partition, KafkaBrokerConnection> resolveBrokers(final Collection<Partition> topicsAndPartitions) {
		return FastList.newList(topicsAndPartitions).toMap(Functions.<Partition>getPassThru(), new Function<Partition, KafkaBrokerConnection>() {
			@Override
			public KafkaBrokerConnection valueOf(Partition partition) {
				return getLeaderConnection(partition);
			}
		});
	}

	public List<Partition> getPartitions(KafkaBrokerAddress kafkaBrokerAddress) {
		return partitionBrokerMapReference.get().getPartitionsByBroker().get(kafkaBrokerAddress).toList();
	}

	public KafkaBrokerConnection createConnection(KafkaBrokerAddress kafkaBrokerAddress) {
		return kafkaBrokersCache.getIfAbsentPutWith(kafkaBrokerAddress, new KafkaBrokerInstantiator(), kafkaBrokerAddress);
	}

	public PartitionBrokerMap getPartitionBrokerMap() {
		return partitionBrokerMapReference.get();
	}


	public void refresh() {
		synchronized (partitionBrokerMapReference) {
			for (KafkaBrokerConnection kafkaBrokerConnection : kafkaBrokersCache) {
				kafkaBrokerConnection.close();
			}
			for (KafkaBrokerAddress kafkaBrokerAddress : kafkaBrokerAddresses) {
				KafkaBrokerConnection kafkaBrokerConnection = new KafkaBrokerConnection(kafkaBrokerAddress);
				KafkaResult<KafkaBrokerAddress> leaders = kafkaBrokerConnection.findLeaders(topics);
				if (leaders.getErrors().size() == 0) {
					this.defaultBroker = kafkaBrokerConnection;
					this.partitionBrokerMapReference.set(new PartitionBrokerMap(UnifiedMap.newMap(leaders.getResult())));
				}
			}
		}
	}

	public Collection<Partition> getPartitions() {
		return getPartitionBrokerMap().getBrokersByPartition().keySet();
	}

	public Collection<Partition> getPartitions(String topic) {
		return getPartitionBrokerMap().getPartitionsByTopic().get(topic).toList();
	}

	private class KafkaBrokerInstantiator implements Function<KafkaBrokerAddress, KafkaBrokerConnection> {
		@Override
		public KafkaBrokerConnection valueOf(KafkaBrokerAddress kafkaBrokerAddress) {
			if (KafkaBrokerConnectionFactory.this.defaultBroker.getBrokerAddress().equals(kafkaBrokerAddress)) {
				return KafkaBrokerConnectionFactory.this.defaultBroker;
			}
			else {
				return new KafkaBrokerConnection(kafkaBrokerAddress);
			}
		}
	}
}
