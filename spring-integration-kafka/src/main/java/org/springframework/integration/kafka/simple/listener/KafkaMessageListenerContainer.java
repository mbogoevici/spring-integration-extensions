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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gs.collections.api.RichIterable;
import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.list.ImmutableList;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.api.multimap.list.ImmutableListMultimap;
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.block.function.checked.CheckedFunction;
import com.gs.collections.impl.factory.Lists;
import com.gs.collections.impl.utility.ArrayIterate;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerAddress;
import org.springframework.integration.kafka.simple.connection.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.simple.connection.Partition;
import org.springframework.integration.kafka.simple.consumer.KafkaMessage;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageBatch;
import org.springframework.integration.kafka.simple.consumer.KafkaMessageFetchRequest;
import org.springframework.integration.kafka.simple.offset.OffsetManager;
import org.springframework.integration.kafka.simple.template.KafkaTemplate;
import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 */
public class KafkaMessageListenerContainer implements InitializingBean,SmartLifecycle {

	private final KafkaTemplate kafkaTemplate;

	private final ImmutableList<Partition> partitions;

	private Executor taskExecutor;

	private final AtomicBoolean running = new AtomicBoolean(false);

	private long timeout = 100L;

	private int maxSize = 10000;

	private MessageListener messageListener;

	private ConcurrentMap<Partition, Long> nextFetchOffsets;

	public KafkaMessageListenerContainer(KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory, final OffsetManager offsetManager, Partition[] partitions) {
		Assert.notNull(kafkaBrokerConnectionFactory, "A connection factory must be supplied");
		Assert.notEmpty(partitions, "A list of partitions must be provided");
		this.kafkaTemplate = new KafkaTemplate(kafkaBrokerConnectionFactory);
		this.partitions = Lists.immutable.with(partitions);
		this.nextFetchOffsets = new ConcurrentHashMap<Partition, Long>(this.partitions.toMap(Functions.<Partition>getPassThru(), new CheckedFunction<Partition, Long>() {
			@Override
			public Long safeValueOf(Partition object) throws Exception {
				return offsetManager.getOffset(object);
			}
		}));
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
	public void afterPropertiesSet() throws Exception {
	}

	@Override
	public void start() {
		this.running.set(true);

		ImmutableListMultimap<KafkaBrokerAddress, Partition> partitionsByBroker = this.partitions.groupBy(new Function<Partition, KafkaBrokerAddress>() {
			@Override
			public KafkaBrokerAddress valueOf(Partition partition) {
				return kafkaTemplate.getKafkaBrokerConnectionFactory().getLeader(partition);
			}
		});
		MutableMap<KafkaBrokerAddress, RichIterable<Partition>> partitionsByBrokerMap = partitionsByBroker.toMap();
		if (taskExecutor == null) {
			taskExecutor = Executors.newFixedThreadPool(partitionsByBrokerMap.size());
		}
		partitionsByBrokerMap.forEachKeyValue(new Procedure2<KafkaBrokerAddress, RichIterable<Partition>>() {
			@Override
			public void value(KafkaBrokerAddress brokerAddress, RichIterable<Partition> partitions) {
				taskExecutor.execute(new FetchTask(partitions.toList()));
			}
		});

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

		private MutableList<Partition> partitions;

		public FetchTask(MutableList<Partition> partition) {
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
							return new KafkaMessageFetchRequest(partition, nextFetchOffsets.get(partition), maxSize);
						}
					}).toArray(new KafkaMessageFetchRequest[0]));
					for (KafkaMessageBatch batch : receive) {
						long highestFetchedOffset = 0;
						for (KafkaMessage kafkaMessage : batch.getMessages()) {
							messageListener.onMessage(kafkaMessage);
							highestFetchedOffset = Math.max(highestFetchedOffset, kafkaMessage.getNextOffset());
						}
						nextFetchOffsets.replace(batch.getPartition(), highestFetchedOffset);
						// if there are still messages on server, we can go on and retrieve more
						if (highestFetchedOffset < batch.getHighWatermark()) {
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
				}
			}
		}
	}
}
