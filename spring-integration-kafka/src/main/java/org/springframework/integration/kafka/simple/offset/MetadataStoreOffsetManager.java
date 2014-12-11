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

import org.springframework.beans.factory.InitializingBean;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnection;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.simple.connection.KafkaResult;
import org.springframework.integration.kafka.simple.connection.Partition;
import org.springframework.integration.metadata.MetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;

/**
 * An {@link org.springframework.integration.kafka.simple.offset.OffsetManager} that persists offsets into
 * a {@link org.springframework.integration.metadata.MetadataStore}
 *
 * @author Marius Bogoevici
 */
public class MetadataStoreOffsetManager implements OffsetManager {

	private final static Log LOG = LogFactory.getLog(MetadataStoreOffsetManager.class);

	private String consumerId = "spring.kafka";

	private MetadataStore metadataStore = new SimpleMetadataStore();

	private KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory;

	private long referenceTimestamp = -2;

	private MutableMap<Partition, Long> offsets = new ConcurrentHashMap<Partition, Long>();

	public MetadataStoreOffsetManager(KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory) {
		this.kafkaBrokerConnectionFactory = kafkaBrokerConnectionFactory;
		loadOffsets();
	}

	public String getConsumerId() {
		return consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public MetadataStore getMetadataStore() {
		return metadataStore;
	}

	public void setMetadataStore(MetadataStore metadataStore) {
		this.metadataStore = metadataStore;
	}

	public long getReferenceTimestamp() {
		return referenceTimestamp;
	}

	public void setReferenceTimestamp(long referenceTimestamp) {
		this.referenceTimestamp = referenceTimestamp;
	}

	@Override
	public void updateOffset(Partition partition, long offset) {
		metadataStore.put(asKey(partition), Long.toString(offset));
		offsets.put(partition, offset);
	}

	@Override
	public long getOffset(Partition partition) {
		return offsets.get(partition);
	}

	private void loadOffsets() {
		this.offsets = new ConcurrentHashMap<Partition, Long>();
		KafkaBrokerConnection kafkaBrokerConnection = kafkaBrokerConnectionFactory.createConnection(kafkaBrokerConnectionFactory.getBrokerAddresses().get(0));
		List<Partition> partitionsRequiringInitialOffsets = new ArrayList<Partition>();
		for (Partition partition : kafkaBrokerConnectionFactory.getPartitions()) {
			String storedOffsetValueAsString = this.metadataStore.get(asKey(partition));
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
				offsets.put(partition, storedOffsetValue);
			} else {
				partitionsRequiringInitialOffsets.add(partition);
			}
 		}
		if (partitionsRequiringInitialOffsets.size() > 0) {
			KafkaResult<Long> initialOffsets = kafkaBrokerConnection.fetchInitialOffset(referenceTimestamp, partitionsRequiringInitialOffsets.toArray(new Partition[partitionsRequiringInitialOffsets.size()]));
			if (initialOffsets.getErrors().size() == 0) {
				for (Partition partitionsRequiringInitialOffset : partitionsRequiringInitialOffsets) {
					offsets.put(partitionsRequiringInitialOffset, initialOffsets.getResult().get(partitionsRequiringInitialOffset));
				}
			} else {
				throw new IllegalStateException("Cannot load initial offsets");
			}
		}
	}

	private String asKey(Partition partition) {
		return partition.getTopic() + " " + partition.getNumber() + " " + consumerId;
	}
}
