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

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.block.function.checked.CheckedFunction;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.utility.ArrayIterate;

import org.springframework.context.SmartLifecycle;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnection;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.simple.connection.Partition;
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

	private final Partition[] partitions;

	private Executor consumerTaskExecutor = Executors.newSingleThreadExecutor();

	private final AtomicBoolean running = new AtomicBoolean(false);

	private long timeout = 100L;

	private String clientId;

	private int maxSize = 10000;

	private MessageListener messageListener;

	public KafkaMessageListenerContainer(KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory, final OffsetManager offsetManager, Partition[] partitions) {
		this.kafkaTemplate = new KafkaTemplate(kafkaBrokerConnectionFactory);
		this.offsetManager = offsetManager;
		this.partitions = partitions;
	}

	public KafkaMessageListenerContainer(final KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory, OffsetManager offsetManager, String[] topics) {
		this(kafkaBrokerConnectionFactory, offsetManager, ArrayIterate.flatCollect(topics,new CheckedFunction<String, Iterable<Partition>>() {
			@Override
			public Iterable<Partition> safeValueOf(String topic) throws Exception {
				return kafkaBrokerConnectionFactory.getPartitions(topic);
			}
		}).toArray(new Partition[0]));
	}


	public MessageListener getMessageListener() {
		return messageListener;
	}

	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
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
		for (KafkaBrokerConnection kafkaBrokerConnection : this.kafkaTemplate.getAllBrokers()) {
			this.consumerTaskExecutor.execute(new FetchTask(FastList.newList(this.kafkaTemplate.getKafkaBrokerConnectionFactory().getPartitions(kafkaBrokerConnection.getBrokerAddress()))));
		}
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
							messageListener.onMessage(kafkaMessage);
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
