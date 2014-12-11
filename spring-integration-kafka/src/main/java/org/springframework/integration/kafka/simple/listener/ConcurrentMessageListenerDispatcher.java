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

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.function.Function0;
import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;

import org.springframework.context.Lifecycle;
import org.springframework.integration.kafka.simple.connection.Partition;
import org.springframework.integration.kafka.simple.consumer.KafkaMessage;
import org.springframework.integration.kafka.simple.offset.OffsetManager;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 */
public class ConcurrentMessageListenerDispatcher implements Lifecycle {

	private MessageListener delegateListener;

	private final Partition[] partitions;

	private final int consumers;

	private OffsetManager offsetManager;

	private MutableMap<Partition,BlockingQueueMessageListenerExecutor> delegates;

	private int queueSize = 1024;

	private Executor taskExecutor;

	private ErrorHandler errorHandler = new LoggingErrorHandler();

	public ConcurrentMessageListenerDispatcher(MessageListener delegateListener, Partition[] partitions, int consumers, OffsetManager offsetManager) {
		Assert.notEmpty(partitions, "A set of partitions must be provided");
		Assert.isTrue(consumers <= partitions.length, "Consumers must be fewer than partitions");
		Assert.notNull(delegateListener, "A delegate must be provided");
		this.delegateListener = delegateListener;
		this.partitions = partitions;
		this.consumers = consumers;
		this.offsetManager = offsetManager;
	}

	public ErrorHandler getErrorHandler() {
		return errorHandler;
	}

	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	public OffsetManager getOffsetManager() {
		return offsetManager;
	}

	public void setOffsetManager(OffsetManager offsetManager) {
		this.offsetManager = offsetManager;
	}

	public int getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	@Override
	public void start() {
		final UnifiedMap<Integer, BlockingQueueMessageListenerExecutor> messageProcessorAllocator = UnifiedMap.newMap();
		delegates = FastList.newListWith(partitions).toMap(Functions.<Partition>getPassThru(), new Function<Partition, BlockingQueueMessageListenerExecutor>() {
			private AtomicInteger atomicInteger = new AtomicInteger(0);
			@Override
			public BlockingQueueMessageListenerExecutor valueOf(Partition object) {
				return messageProcessorAllocator.getIfAbsentPut(atomicInteger.getAndIncrement() % consumers, new Function0<BlockingQueueMessageListenerExecutor>() {
					@Override
					public BlockingQueueMessageListenerExecutor value() {
						return new BlockingQueueMessageListenerExecutor(queueSize, offsetManager, delegateListener);
					}
				});
			}
		});

		if (this.taskExecutor == null) {
			this.taskExecutor = Executors.newFixedThreadPool(consumers, new CustomizableThreadFactory("dispatcher-"));
		}
		delegates.flip().keyBag().toSet().forEach(new Procedure<BlockingQueueMessageListenerExecutor>() {
			@Override
			public void value(BlockingQueueMessageListenerExecutor delegate) {
				delegate.start();
				taskExecutor.execute(delegate);
			}
		});
	}

	@Override
	public void stop() {
		delegates.forEachValue(new Procedure<BlockingQueueMessageListenerExecutor>() {
			@Override
			public void value(BlockingQueueMessageListenerExecutor delegate) {
				delegate.stop();
			}
		});
	}

	@Override
	public boolean isRunning() {
		return delegates.getLast().isRunning();
	}

	public void dispatch(KafkaMessage message) {
		try {
			delegates.get(message.getPartition()).onMessage(message);
			this.offsetManager.updateOffset(message.getPartition(), message.getNextOffset());
		}
		catch (Exception e) {
			errorHandler.handleError(e);
		}
	}

}
