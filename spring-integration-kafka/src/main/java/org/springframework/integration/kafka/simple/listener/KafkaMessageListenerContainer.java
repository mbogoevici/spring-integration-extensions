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


package org.springframework.integration.kafka.simple.listener;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.SmartLifecycle;
import org.springframework.integration.kafka.simple.consumer.KafkaConfiguration;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageFetchRequest;
import org.springframework.integration.kafka.simple.consumer.KafkaTemplate;
import org.springframework.integration.kafka.simple.offset.OffsetManager;
import org.springframework.integration.kafka.simple.model.Partition;
import org.springframework.integration.kafka.simple.model.KafkaMessage;
import org.springframework.integration.metadata.MetadataStore;

/**
 * @author Marius Bogoevici
 */
public class KafkaMessageListenerContainer implements SmartLifecycle {

	private final KafkaTemplate kafkaTemplate;

	private final OffsetManager offsetManager;

	private MessageListener messageListener;

	private Executor consumerTaskExecutor = Executors.newSingleThreadExecutor();

	private Executor processorTaskExecutor = Executors.newSingleThreadExecutor();

	private final AtomicBoolean running = new AtomicBoolean(false);

	private final Partition partition;


	private long referencePoint;

	private long timeout = 100L;

	private String clientId;

	private int maxSize = 10000;

	private BlockingQueueMessageProcessor messageProcessor;

	public KafkaMessageListenerContainer(KafkaConfiguration kafkaConfiguration, MetadataStore metadataStore, Partition partition, long referencePoint) {
		this.referencePoint = referencePoint;
		this.kafkaTemplate = new KafkaTemplate(kafkaConfiguration);
		this.offsetManager = new OffsetManager(kafkaConfiguration, kafkaTemplate.getKafkaResolver(), metadataStore, referencePoint);
		this.partition  = partition;
	}

	public MessageListener getMessageListener() {
		return messageListener;
	}

	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
	}

	public Executor getTaskExecutor() {
		return consumerTaskExecutor;
	}

	public void setTaskExecutor(Executor taskExecutor) {
		this.consumerTaskExecutor = taskExecutor;
	}


	@Override
	public boolean isAutoStartup() {
		return false;
	}

	@Override
	public void stop(Runnable callback) {
		this.running.set(false);
	}

	@Override
	public void start() {
		this.running.set(true);
		this.consumerTaskExecutor.execute(new FetchTask());
		messageProcessor = new BlockingQueueMessageProcessor(10, offsetManager);
		messageProcessor.setMessageListener(messageListener);
		messageProcessor.start();
		this.processorTaskExecutor.execute(messageProcessor);
	}

	@Override
	public void stop() {
		this.running.set(false);
	}

	@Override
	public boolean isRunning() {
		return this.running.get();
	}

	@Override
	public int getPhase() {
		return 0;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public class FetchTask implements Runnable {
		@Override
		public void run() {
			KafkaMessageListenerContainer kafkaMessageListenerContainer = KafkaMessageListenerContainer.this;
			while(running.get()) {
				Set<Partition> partitionsWithData = new HashSet<Partition>();
				do {
					partitionsWithData.add(partition);
					Iterable<KafkaMessage> receive = kafkaTemplate.receive(new KafkaMessageFetchRequest(partition, offsetManager.getOffset(partition), maxSize));
					boolean hasData = false;
					for (KafkaMessage message : receive) {
						try {
							messageProcessor.processMessage(message);
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							if (!kafkaMessageListenerContainer.running.get()) {
								// no longer running
								return;
							}
							throw new IllegalStateException(e);
						}
						if (message.getHighWaterMark() == message.getNextOffset()) {
							partitionsWithData.remove(partition);
						}
						hasData = true;
					}
					if (!hasData) {
						partitionsWithData.remove(partition);
					}
				} while (!partitionsWithData.isEmpty());
				try {
					Thread.currentThread().sleep(timeout);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					if (!kafkaMessageListenerContainer.running.get()) {
						// no longer running
						return;
					}
					throw new IllegalStateException(e);
				}
			}
		}
	}
}
