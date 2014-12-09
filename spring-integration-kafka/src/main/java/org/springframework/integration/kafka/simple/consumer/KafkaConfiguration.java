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


package org.springframework.integration.kafka.simple.consumer;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.integration.kafka.simple.connection.KafkaBrokerAddress;
import org.springframework.integration.kafka.simple.connection.Partition;

/**
 * @author Marius Bogoevici
 */
public class KafkaConfiguration {

	private String consumerId;

	private List<KafkaBrokerAddress> brokerAddresses;

	private List<Partition> partitions;

	public KafkaConfiguration(List<KafkaBrokerAddress> brokerAddresses, List<Partition> Partitions) {
		this.brokerAddresses = brokerAddresses;
		this.partitions = Partitions;
	}

	public String getConsumerId() {
		return consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public List<KafkaBrokerAddress> getBrokerAddresses() {
		return brokerAddresses;
	}

	public void setBrokerAddresses(List<KafkaBrokerAddress> brokerAddresses) {
		this.brokerAddresses = brokerAddresses;
	}

	public List<Partition> getPartitions() {
		return partitions;
	}

	public List<String> getTopics() {
		Set<String> topics = new LinkedHashSet<String>();
		for (Partition partition : partitions) {
			topics.add(partition.getTopic());
		}
		return new ArrayList<String>(topics);
	}

	public void setPartitions(List<Partition> partitions) {
		this.partitions = partitions;
	}

}
