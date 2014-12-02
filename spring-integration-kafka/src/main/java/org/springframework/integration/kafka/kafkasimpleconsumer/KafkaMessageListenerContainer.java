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

import java.util.concurrent.Executor;

import org.springframework.integration.metadata.MetadataStore;

/**
 * @author Marius Bogoevici
 */
public class KafkaMessageListenerContainer {

	private final KafkaTemplate kafkaTemplate;

	private final OffsetManager offsetManager;

	private MessageListener messageListener;

	private Executor taskExecutor;

	public KafkaMessageListenerContainer(KafkaConfiguration kafkaConfiguration, MetadataStore metadataStore) {
		this.kafkaTemplate = new KafkaTemplate(kafkaConfiguration);
		this.offsetManager = new OffsetManager(kafkaConfiguration, kafkaTemplate.getKafkaResolver(), metadataStore);
	}

	public MessageListener getMessageListener() {
		return messageListener;
	}

	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
	}

	public Executor getTaskExecutor() {
		return taskExecutor;
	}

	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

}
