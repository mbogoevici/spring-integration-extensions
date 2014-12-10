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

import java.util.concurrent.atomic.AtomicInteger;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.function.Function0;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.integration.kafka.simple.connection.Partition;
import org.springframework.integration.kafka.simple.consumer.KafkaMessage;
import org.springframework.integration.kafka.simple.offset.OffsetManager;

/**
 * @author Marius Bogoevici
 */
public class DispatchingMessageListener implements MessageListener, InitializingBean {

	private MessageListener delegateListener;

	private Partition[] partitions;

	private int consumers;

	private OffsetManager offsetManager;

	private MutableMap<Partition, MessageListener> delegates;

	public DispatchingMessageListener(Partition[] partitions, int consumers, OffsetManager offsetManager) {
		this.partitions = partitions;
		this.consumers = consumers;
		this.offsetManager = offsetManager;
	}

	public MessageListener getDelegateListener() {
		return delegateListener;
	}

	public void setDelegateListener(MessageListener delegate) {
		this.delegateListener = delegate;
	}

	public OffsetManager getOffsetManager() {
		return offsetManager;
	}

	public void setOffsetManager(OffsetManager offsetManager) {
		this.offsetManager = offsetManager;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		final UnifiedMap<Integer, BlockingQueueMessageListener> messageProcessorAllocator = UnifiedMap.newMap();

		delegates = FastList.newListWith(partitions).toMap(Functions.<Partition>getPassThru(), new Function<Partition, MessageListener>() {
			private AtomicInteger atomicInteger = new AtomicInteger(0);

			@Override
			public MessageListener valueOf(Partition object) {
				return messageProcessorAllocator.getIfAbsentPut(atomicInteger.getAndIncrement() % consumers, new Function0<BlockingQueueMessageListener>() {
					@Override
					public BlockingQueueMessageListener value() {
						BlockingQueueMessageListener blockingQueueMessageListener = new BlockingQueueMessageListener(100, offsetManager);
						blockingQueueMessageListener.setDelegate(delegateListener);
						blockingQueueMessageListener.start();
						return blockingQueueMessageListener;
					}
				});
			}
		});
	}

	@Override
	public void onMessage(KafkaMessage message) {
		delegates.get(message.getPartition()).onMessage(message);
	}
}
