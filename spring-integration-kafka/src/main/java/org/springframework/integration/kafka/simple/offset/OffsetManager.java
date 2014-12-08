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


package org.springframework.integration.kafka.simple.offset;

import java.util.ArrayList;
import java.util.List;

import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnection;
import org.springframework.integration.kafka.simple.consumer.KafkaConfiguration;
import org.springframework.integration.kafka.simple.connection.KafkaResolver;
import org.springframework.integration.kafka.simple.connection.KafkaResult;
import org.springframework.integration.kafka.simple.model.Offset;
import org.springframework.integration.kafka.simple.model.Partition;
import org.springframework.integration.metadata.MetadataStore;

/**
 * @author Marius Bogoevici
 */
public class OffsetManager {

	private final static Log LOG = LogFactory.getLog(OffsetManager.class);

	private KafkaConfiguration kafkaConfiguration;

	private MetadataStore metadataStore;

	private KafkaResolver kafkaResolver;

	private long referencePoint;

	private MutableMap<Partition, Long> offsets = new ConcurrentHashMap<Partition, Long>();

	public OffsetManager(KafkaConfiguration kafkaConfiguration, KafkaResolver kafkaResolver, MetadataStore metadataStore, long referencePoint) {
		this.kafkaConfiguration = kafkaConfiguration;
		this.metadataStore = metadataStore;
		this.kafkaResolver = kafkaResolver;
		this.referencePoint = referencePoint;
		this.offsets = new ConcurrentHashMap<Partition, Long>();
		loadOffsets();
	}


	public void updateOffset(Offset offset) {
		metadataStore.put(asKey(offset.getPartition()), Long.toString(offset.getOffset()));
		offsets.put(offset.getPartition(), offset.getOffset());
	}

	public long getOffset(Partition partition) {
		return offsets.get(partition);
	}

	private void loadOffsets() {
		KafkaBrokerConnection kafkaBrokerConnection = kafkaResolver.resolveAddress(kafkaConfiguration.getBrokerAddresses().get(0));
		List<Partition> partitionsRequiringInitialOffsets = new ArrayList<Partition>();
		for (Partition Partition : kafkaConfiguration.getPartitions()) {
			String storedOffsetValueAsString = this.metadataStore.get(asKey(Partition));
			Long storedOffsetValue = null;
			if (storedOffsetValueAsString != null) {
				try {
					storedOffsetValue = Long.parseLong(storedOffsetValueAsString);
				}
				catch (NumberFormatException e) {
					LOG.warn("Invalid key: " + storedOffsetValue);
				}
			}
			if (storedOffsetValue != null) {
				offsets.put(Partition, storedOffsetValue);
			} else {
				partitionsRequiringInitialOffsets.add(Partition);
			}
 		}
		if (partitionsRequiringInitialOffsets.size() > 0) {
			KafkaResult<Long> initialOffsets = kafkaBrokerConnection.fetchInitialOffset(referencePoint, partitionsRequiringInitialOffsets.toArray(new Partition[partitionsRequiringInitialOffsets.size()]));
			if (initialOffsets.getErrors().size() == 0) {
				for (Partition partitionsRequiringInitialOffset : partitionsRequiringInitialOffsets) {
					offsets.put(partitionsRequiringInitialOffset, initialOffsets.getResult().get(partitionsRequiringInitialOffset));
				}
			} else {
				throw new IllegalStateException("Cannot load initial offsets");
			}
		}
	}

	public String asKey(Partition partition) {
		return partition.getNumber() + " " + partition.getTopic();
	}
}
