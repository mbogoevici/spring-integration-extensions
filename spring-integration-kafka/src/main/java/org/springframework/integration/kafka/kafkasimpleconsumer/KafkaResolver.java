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
import java.util.concurrent.atomic.AtomicReference;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.multimap.Multimap;
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.multimap.list.FastListMultimap;

import org.springframework.util.ObjectUtils;

/**
 * @author Marius Bogoevici
 */
public class KafkaResolver {

	private final String[] topics;

	private final FastList<KafkaBrokerAddress> kafkaBrokerAddresses;

	private UnifiedMap<KafkaBrokerAddress, KafkaBrokerConnection> kafkaBrokersCache = UnifiedMap.newMap();

	private final AtomicReference<PartitionBrokerTable> partitionBrokerTableReference = new AtomicReference<PartitionBrokerTable>();

	private KafkaBrokerConnection adminBroker;

	/**
	 * @param kafkaBrokerAddresses
	 * @param partitions
	 */
	public KafkaResolver(List<KafkaBrokerAddress> kafkaBrokerAddresses, Partition... partitions) {
		this.kafkaBrokerAddresses = FastList.newList(kafkaBrokerAddresses);
		this.topics = FastList.newListWith(partitions).collect(new Function<Partition, String>() {
			@Override
			public String valueOf(Partition partition) {
				return partition.getTopic();
			}
		}).distinct().toTypedArray(String.class);
		refresh();
	}

	public KafkaBrokerConnection getAdminBroker() {
		return adminBroker;
	}

	/**
	 * Resolves the broker associated with a specific topic and partition. Internally,
	 * caches the {@link KafkaBrokerConnection}
	 *
	 * @param partition
	 * @return the broker associated with the provided topic and partition
	 */
	public KafkaBrokerConnection resolveBroker(Partition partition) {
		KafkaBrokerAddress kafkaBrokerAddress = partitionBrokerTableReference.get().getBrokersByPartition().get(partition);
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

	public List<Partition> resolvePartitions(KafkaBrokerAddress kafkaBrokerAddress) {
		return partitionBrokerTableReference.get().getPartitionsByBroker().get(kafkaBrokerAddress).toList();
	}

	public KafkaBrokerConnection resolveAddress(KafkaBrokerAddress kafkaBrokerAddress) {
		return kafkaBrokersCache.getIfAbsentPutWith(kafkaBrokerAddress, new KafkaBrokerInstantiator(), kafkaBrokerAddress);
	}

	public void refresh() {
		synchronized (partitionBrokerTableReference) {
			for (KafkaBrokerAddress kafkaBrokerAddress : kafkaBrokerAddresses) {
				KafkaBrokerConnection kafkaBrokerConnection = new KafkaBrokerConnection(kafkaBrokerAddress);
				KafkaResult<KafkaBrokerAddress> leaders = kafkaBrokerConnection.findLeaders(topics);
				if (leaders.getErrors().size() == 0) {
					this.adminBroker = kafkaBrokerConnection;
					this.partitionBrokerTableReference.set(new PartitionBrokerTable(UnifiedMap.newMap(leaders.getResult())));
				}
			}
		}
	}

	private class KafkaBrokerInstantiator implements Function<KafkaBrokerAddress, KafkaBrokerConnection> {
		@Override
		public KafkaBrokerConnection valueOf(KafkaBrokerAddress kafkaBrokerAddress) {
			if (KafkaResolver.this.adminBroker.getBrokerAddress().equals(kafkaBrokerAddress)) {
				return KafkaResolver.this.adminBroker;
			}
			else {
				return new KafkaBrokerConnection(kafkaBrokerAddress);
			}
		}
	}
}
