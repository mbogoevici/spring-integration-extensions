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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.context.Lifecycle;

/**
 * @author Marius Bogoevici
 */
public class BlockingQueueMessageProcessor implements MessageProcessor,Runnable,Lifecycle {

	private BlockingQueue<KafkaMessage> messages;

	private AtomicBoolean running = new AtomicBoolean(false);

	private MessageListener messageListener;

	private OffsetManager offsetManager;

	public MessageListener getMessageListener() {
		return messageListener;
	}

	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
	}

	public BlockingQueueMessageProcessor(int capacity, OffsetManager offsetManager) {
		this.offsetManager = offsetManager;
		this.messages = new ArrayBlockingQueue<KafkaMessage>(capacity, true);
	}

	public BlockingQueue<KafkaMessage> getMessages() {
		return messages;
	}

	public void setMessages(BlockingQueue<KafkaMessage> messages) {
		this.messages = messages;
	}

	@Override
	public void processMessage(KafkaMessage message) throws InterruptedException{
		this.messages.put(message);
	}

	@Override
	public void start() {
		this.running.set(true);
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
	public void run() {
		while(this.running.get()) {
			try {
				KafkaMessage nextMessage = this.getMessages().take();
				messageListener.onMessage(nextMessage);
				offsetManager.updateOffset(new Offset(nextMessage.getPartition(), nextMessage.getNextOffset()));
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}
}
