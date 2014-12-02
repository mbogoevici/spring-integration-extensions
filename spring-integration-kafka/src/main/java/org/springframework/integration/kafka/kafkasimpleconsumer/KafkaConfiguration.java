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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import kafka.common.TopicAndPartition;

/**
 * @author Marius Bogoevici
 */
public class KafkaConfiguration {

	private List<KafkaBrokerAddress> brokerAddresses;

	private List<TopicAndPartition> topicAndPartitions;

	public KafkaConfiguration(List<KafkaBrokerAddress> brokerAddresses, List<TopicAndPartition> topicAndPartitions) {
		this.brokerAddresses = brokerAddresses;
		this.topicAndPartitions = topicAndPartitions;
	}

	public List<KafkaBrokerAddress> getBrokerAddresses() {
		return brokerAddresses;
	}

	public void setBrokerAddresses(List<KafkaBrokerAddress> brokerAddresses) {
		this.brokerAddresses = brokerAddresses;
	}

	public List<TopicAndPartition> getTopicAndPartitions() {
		return topicAndPartitions;
	}

	public List<String> getTopics() {
		Set<String> topics = new LinkedHashSet<String>();
		for (TopicAndPartition topicAndPartition : topicAndPartitions) {
			topics.add(topicAndPartition.topic());
		}
		return new ArrayList<String>(topics);
	}

	public void setTopicAndPartitions(List<TopicAndPartition> topicAndPartitions) {
		this.topicAndPartitions = topicAndPartitions;
	}

}
