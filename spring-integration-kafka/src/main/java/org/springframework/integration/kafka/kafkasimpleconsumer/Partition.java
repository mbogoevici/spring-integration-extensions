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

/**
 * @author Marius Bogoevici
 */
public class Partition {

	private String topic;

	private int number;

	public Partition(String topic, int number) {
		this.topic = topic;
		this.number = number;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Partition partition = (Partition) o;

		if (number != partition.number) return false;
		if (!topic.equals(partition.topic)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = topic.hashCode();
		result = 31 * result + number;
		return result;
	}
}
