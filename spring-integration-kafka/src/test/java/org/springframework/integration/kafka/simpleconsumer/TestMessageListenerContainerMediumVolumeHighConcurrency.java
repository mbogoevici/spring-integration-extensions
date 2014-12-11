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


package org.springframework.integration.kafka.simpleconsumer;

import com.gs.collections.api.multimap.MutableMultimap;
import com.gs.collections.impl.factory.Multimaps;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Marius Bogoevici
 */
public class TestMessageListenerContainerMediumVolumeHighConcurrency extends AbstractMessageListenerContainerTest {

	public static final int PARTITION_COUNT = 100;

	@BeforeClass
	public static void setUp() throws Exception {
		MutableMultimap<Integer, Integer> partitionDistribution = Multimaps.mutable.list.with();
		for (int i = 0; i < PARTITION_COUNT; i++) {
			partitionDistribution.put(i,0);
		}
		createTopic(TEST_TOPIC, partitionDistribution);
	}

	@Test
	public void testMessageListenerContainer() throws Exception {
		runMessageListenerTest(100, 20, PARTITION_COUNT, 10000);
	}

}
