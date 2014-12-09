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
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.list.mutable.FastList;

import org.springframework.context.SmartLifecycle;
import org.springframework.integration.kafka.simple.connection.Partition;
import org.springframework.integration.kafka.simple.consumer.KafkaConfiguration;
import org.springframework.integration.kafka.simple.consumer.KafkaMessage;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageBatch;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageFetchRequest;
import org.springframework.integration.kafka.simple.offset.OffsetManager;
import org.springframework.integration.kafka.simple.template.KafkaTemplate;

/**
 * @author Marius Bogoevici
 */
public class KafkaMessageListenerContainer implements SmartLifecycle {

	private final KafkaTemplate kafkaTemplate;

	private final OffsetManager offsetManager;

	private Executor consumerTaskExecutor = Executors.newSingleThreadExecutor();

	private final AtomicBoolean running = new AtomicBoolean(false);

	private final FastList<Partition> partitions;

	private long referencePoint;

	private long timeout = 100L;

	private String clientId;

	private int maxSize = 10000;

	private MessageProcessor messageProcessor;

	public KafkaMessageListenerContainer(KafkaConfiguration kafkaConfiguration, OffsetManager offsetManager, List<Partition> partitions, long referencePoint) {
		this.referencePoint = referencePoint;
		this.kafkaTemplate = new KafkaTemplate(kafkaConfiguration);
		this.offsetManager = offsetManager;
		this.partitions = FastList.newList(partitions);
	}

	public MessageProcessor getMessageProcessor() {
		return messageProcessor;
	}

	public void setMessageProcessor(MessageProcessor messageProcessor) {
		this.messageProcessor = messageProcessor;
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
		this.consumerTaskExecutor.execute(new FetchTask(partitions));
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

		private FastList<Partition> partitions;

		public FetchTask(FastList<Partition> partition) {
			this.partitions = partition;
		}

		@Override
		public void run() {
			KafkaMessageListenerContainer kafkaMessageListenerContainer = KafkaMessageListenerContainer.this;
			while (running.get()) {
				Set<Partition> partitionsWithRemainingData = new HashSet<Partition>();
				do {
					Iterable<KafkaMessageBatch> receive = kafkaTemplate.receive(this.partitions.collect(new Function<Partition, KafkaMessageFetchRequest>() {
						@Override
						public KafkaMessageFetchRequest valueOf(Partition partition) {
							return new KafkaMessageFetchRequest(partition, offsetManager.getOffset(partition), maxSize);
						}
					}).toTypedArray(KafkaMessageFetchRequest.class));
					for (KafkaMessageBatch batch : receive) {
						long highestOffset = 0;
						for (KafkaMessage kafkaMessage : batch.getMessages()) {
							messageProcessor.processMessage(kafkaMessage);
							highestOffset = Math.max(highestOffset, kafkaMessage.getNextOffset());
						}
						// if there are still messages on server, we can go on and retrieve more
						if (highestOffset < batch.getHighWatermark()) {
							partitionsWithRemainingData.add(batch.getPartition());
						}
					}
				} while (!partitionsWithRemainingData.isEmpty());
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
