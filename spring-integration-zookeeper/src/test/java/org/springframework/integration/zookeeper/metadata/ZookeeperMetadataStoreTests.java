/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.integration.zookeeper.metadata;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.springframework.integration.zookeeper.metadata.EqualsResultMatcher.equalsResult;
import static org.springframework.integration.zookeeper.metadata.EventuallyMatcher.eventually;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.gs.collections.impl.factory.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.integration.zookeeper.metadata.EqualsResultMatcher.Evaluator;

/**
 * @author Marius Bogoevici
 */
public class ZookeeperMetadataStoreTests {

	private static final Log log = LogFactory.getLog(ZookeeperMetadataStore.class);

	private TestingServer testingServer;

	private CuratorFramework client;

	private ZookeeperMetadataStore metadataStore;

	@Before
	public void setUp() throws Exception{
		testingServer = new TestingServer(true);
		client = createNewClient();
		metadataStore = new ZookeeperMetadataStore(client);
		metadataStore.afterPropertiesSet();
	}

	@After
	public void tearDown() {
		closeMetadataStore(this.metadataStore);
		closeClient(this.client);
		try {
			testingServer.stop();
		}
		catch (IOException e) {
			log.warn("Exception thrown while shutting down ZooKeeper: ", e);
		}
		testingServer.getTempDirectory().delete();
	}


	@Test
	public void testGetNonExistingKeyValue() {
		String retrievedValue = metadataStore.get("does-not-exist");
		assertNull(retrievedValue);
	}

	@Test
	public void testPersistKeyValue() throws Exception {
		String testKey = "ZookeeperMetadataStoreTests-Persist";
		metadataStore.put(testKey, "Integration");
		assertNotNull(client.checkExists().forPath(metadataStore.getPath(testKey)));
		assertEquals("Integration",
				Conversions.bytesToString(client.getData().forPath(metadataStore.getPath(testKey)),"UTF-8"));
	}


	@Test
	public void testGetValueFromMetadataStore() throws Exception {
		String testKey = "ZookeeperMetadataStoreTests-GetValue";
		metadataStore.put(testKey, "Hello Zookeeper");
		String retrievedValue = metadataStore.get(testKey);
		assertEquals("Hello Zookeeper", retrievedValue);
	}


	@Test
	public void testPutIfAbsent() throws Exception {
		final String testKey = "ZookeeperMetadataStoreTests-Persist";
		final String testKey2 = "ZookeeperMetadataStoreTests-Persist-2";
		metadataStore.put(testKey, "Integration");
		assertNotNull(client.checkExists().forPath(metadataStore.getPath(testKey)));
		assertEquals("Integration",
				Conversions.bytesToString(client.getData().forPath(metadataStore.getPath(testKey)),"UTF-8"));
		CuratorFramework otherClient = createNewClient();
		final ZookeeperMetadataStore otherMetadataStore = new ZookeeperMetadataStore(otherClient);
		otherMetadataStore.afterPropertiesSet();
		otherMetadataStore.putIfAbsent(testKey, "OtherValue");
		assertEquals("Integration",
				Conversions.bytesToString(client.getData().forPath(metadataStore.getPath(testKey)), "UTF-8"));
		assertEquals("Integration", metadataStore.get(testKey));
		assertThat("Integration", eventually(equalsResult(new Evaluator<String>() {
			@Override
			public String evaluate() {
				return otherMetadataStore.get(testKey);
			}
		})));
		otherMetadataStore.putIfAbsent(testKey2, "Integration-2");
		assertEquals("Integration-2",
				Conversions.bytesToString(client.getData().forPath(metadataStore.getPath(testKey2)),"UTF-8"));
		assertEquals("Integration-2", otherMetadataStore.get(testKey2));
		assertThat("Integration-2", eventually(equalsResult(new Evaluator<String>() {
			@Override
			public String evaluate() {
				return metadataStore.get(testKey2);
			}
		})));
		closeClient(otherClient);
	}

	@Test
	public void testReplace() throws Exception {
		final String testKey = "ZookeeperMetadataStoreTests-Replace";
		metadataStore.put(testKey, "Integration");
		assertNotNull(client.checkExists().forPath(metadataStore.getPath(testKey)));
		assertEquals("Integration",
				Conversions.bytesToString(client.getData().forPath(metadataStore.getPath(testKey)),"UTF-8"));
		CuratorFramework otherClient = createNewClient();
		final ZookeeperMetadataStore otherMetadataStore = new ZookeeperMetadataStore(otherClient);
		otherMetadataStore.afterPropertiesSet();
		otherMetadataStore.replace(testKey, "OtherValue", "Integration-2");
		assertEquals("Integration",
				Conversions.bytesToString(client.getData().forPath(metadataStore.getPath(testKey)), "UTF-8"));
		assertEquals("Integration", metadataStore.get(testKey));
		assertThat("Integration", eventually(equalsResult(new Evaluator<String>() {
			@Override
			public String evaluate() {
				return otherMetadataStore.get(testKey);
			}
		})));
		otherMetadataStore.replace(testKey, "Integration", "Integration-2");
		assertEquals("Integration-2",
				Conversions.bytesToString(client.getData().forPath(metadataStore.getPath(testKey)), "UTF-8"));
		assertThat("Integration-2", eventually(equalsResult(new Evaluator<String>() {
			@Override
			public String evaluate() {
				return metadataStore.get(testKey);
			}
		})));
		assertEquals("Integration-2", otherMetadataStore.get(testKey));
		closeClient(otherClient);
	}

	@Test
	public void testPersistEmptyStringToMetadataStore() {
		String testKey = "ZookeeperMetadataStoreTests-PersistEmpty";
		metadataStore.put(testKey, "");
		assertEquals("", metadataStore.get(testKey));
	}

	@Test
	public void testPersistNullStringToMetadataStore() {
		try {
			metadataStore.put("ZookeeperMetadataStoreTests-PersistEmpty", null);
		}
		catch (IllegalArgumentException e) {
			assertEquals("'value' must not be null.", e.getMessage());
			return;
		}
		fail("Expected an IllegalArgumentException to be thrown.");
	}

	@Test
	public void testPersistWithEmptyKeyToMetadataStore() {
		metadataStore.put("", "PersistWithEmptyKey");
		String retrievedValue = metadataStore.get("");
		assertEquals("PersistWithEmptyKey", retrievedValue);
	}

	@Test
	public void testPersistWithNullKeyToMetadataStore() {
		try {
			metadataStore.put(null, "something");
		}
		catch (IllegalArgumentException e) {
			assertEquals("'key' must not be null.", e.getMessage());
			return;
		}
		fail("Expected an IllegalArgumentException to be thrown.");
	}

	@Test
	public void testGetValueWithNullKeyFromMetadataStore() {
		try {
			metadataStore.get(null);
		}
		catch (IllegalArgumentException e) {
			assertEquals("'key' must not be null.", e.getMessage());
			return;
		}
		fail("Expected an IllegalArgumentException to be thrown.");
	}

	@Test
	public void testRemoveFromMetadataStore() throws Exception {
		String testKey = "ZookeeperMetadataStoreTests-Remove";
		String testValue = "Integration";
		metadataStore.put(testKey, testValue);
		assertEquals(testValue, metadataStore.remove(testKey));
		Thread.sleep(1000);
		assertNull(metadataStore.remove(testKey));
	}

	@Test
	public void testListenerInvokedOnLocalChanges() throws Exception {
		String testKey = "ZookeeperMetadataStoreTests";

		// register listeners
		final List<List<String>> notifiedChanges = Lists.mutable.of();
		final Map<String,CyclicBarrier> barriers = new HashMap<>();
		barriers.put("add", new CyclicBarrier(2));
		barriers.put("remove", new CyclicBarrier(2));
		barriers.put("update", new CyclicBarrier(2));
		metadataStore.addListener(new MessageStoreListenerAdapter() {
			@Override
			public void onAdd(String key, String value) {
				notifiedChanges.add(Lists.immutable.of("add", key, value).castToList());
				waitAtBarrier("add", barriers);
			}

			@Override
			public void onRemove(String key, String oldValue) {
				notifiedChanges.add(Lists.immutable.of("remove", key, oldValue).castToList());
				waitAtBarrier("remove", barriers);
			}

			@Override
			public void onUpdate(String key, String newValue) {
				notifiedChanges.add(Lists.immutable.of("update", key, newValue).castToList());
				waitAtBarrier("update", barriers);
			}
		});

		// the tests themselves
		barriers.get("add").reset();
		metadataStore.put(testKey, "Integration");
		waitAtBarrier("add", barriers);
		assertThat(notifiedChanges, hasSize(1));
		assertThat(notifiedChanges.get(0), IsIterableContainingInOrder.contains("add", testKey, "Integration"));

		metadataStore.putIfAbsent(testKey, "Integration++");
		// there is no update and therefore we expect no changes
		assertThat(notifiedChanges, hasSize(1));

		barriers.get("update").reset();
		metadataStore.put(testKey, "Integration-2");
		waitAtBarrier("update", barriers);
		assertThat(notifiedChanges, hasSize(2));
		assertThat(notifiedChanges.get(1), IsIterableContainingInOrder.contains("update", testKey, "Integration-2"));

		barriers.get("update").reset();
		metadataStore.replace(testKey, "Integration-2", "Integration-3");
		waitAtBarrier("update", barriers);
		assertThat(notifiedChanges, hasSize(3));
		assertThat(notifiedChanges.get(2), IsIterableContainingInOrder.contains("update", testKey, "Integration-3"));

		metadataStore.replace(testKey, "Integration-2", "Integration-none");
		assertThat(notifiedChanges, hasSize(3));

		barriers.get("remove").reset();
		metadataStore.remove(testKey);
		waitAtBarrier("remove", barriers);
		assertThat(notifiedChanges, hasSize(4));
		assertThat(notifiedChanges.get(3), IsIterableContainingInOrder.contains("remove", testKey, "Integration-3"));

		// sleep and try to see if there were any other updates
		Thread.sleep(1000);
		assertThat(notifiedChanges, hasSize(4));
	}

	@Test
	public void testListenerInvokedOnRemoteChanges() throws Exception {
		String testKey = "ZookeeperMetadataStoreTests";

		CuratorFramework otherClient = createNewClient();
		ZookeeperMetadataStore otherMetadataStore = new ZookeeperMetadataStore(otherClient);

		// register listeners
		final List<List<String>> notifiedChanges = Lists.mutable.of();
		final Map<String,CyclicBarrier> barriers = new HashMap<>();
		barriers.put("add", new CyclicBarrier(2));
		barriers.put("remove", new CyclicBarrier(2));
		barriers.put("update", new CyclicBarrier(2));
		metadataStore.addListener(new MessageStoreListenerAdapter() {
			@Override
			public void onAdd(String key, String value) {
				notifiedChanges.add(Lists.immutable.of("add", key, value).castToList());
				waitAtBarrier("add", barriers);
			}

			@Override
			public void onRemove(String key, String oldValue) {
				notifiedChanges.add(Lists.immutable.of("remove", key, oldValue).castToList());
				waitAtBarrier("remove", barriers);
			}

			@Override
			public void onUpdate(String key, String newValue) {
				notifiedChanges.add(Lists.immutable.of("update", key, newValue).castToList());
				waitAtBarrier("update", barriers);
			}
		});

		// the tests themselves
		barriers.get("add").reset();
		otherMetadataStore.put(testKey, "Integration");
		waitAtBarrier("add", barriers);
		assertThat(notifiedChanges, hasSize(1));
		assertThat(notifiedChanges.get(0), IsIterableContainingInOrder.contains("add", testKey, "Integration"));

		otherMetadataStore.putIfAbsent(testKey, "Integration++");
		// there is no update and therefore we expect no changes
		assertThat(notifiedChanges, hasSize(1));

		barriers.get("update").reset();
		otherMetadataStore.put(testKey, "Integration-2");
		waitAtBarrier("update", barriers);
		assertThat(notifiedChanges, hasSize(2));
		assertThat(notifiedChanges.get(1), IsIterableContainingInOrder.contains("update", testKey, "Integration-2"));

		barriers.get("update").reset();
		otherMetadataStore.replace(testKey, "Integration-2", "Integration-3");
		waitAtBarrier("update", barriers);
		assertThat(notifiedChanges, hasSize(3));
		assertThat(notifiedChanges.get(2), IsIterableContainingInOrder.contains("update", testKey, "Integration-3"));

		otherMetadataStore.replace(testKey, "Integration-2", "Integration-none");
		assertThat(notifiedChanges, hasSize(3));

		barriers.get("remove").reset();
		otherMetadataStore.remove(testKey);
		waitAtBarrier("remove", barriers);
		assertThat(notifiedChanges, hasSize(4));
		assertThat(notifiedChanges.get(3), IsIterableContainingInOrder.contains("remove", testKey, "Integration-3"));

		// sleep and try to see if there were any other updates - if there any pending updates, we should catch them by now
		Thread.sleep(1000);
		assertThat(notifiedChanges, hasSize(4));
	}

	@Test
	public void testAddRemoveListener() throws Exception {
		MetadataStoreListener mockListener = Mockito.mock(MetadataStoreListener.class);
		DirectFieldAccessor accessor = new DirectFieldAccessor(metadataStore);

		@SuppressWarnings("unchecked")
		List<MetadataStoreListener> listeners = (List<MetadataStoreListener>) accessor.getPropertyValue("listeners");

		assertThat(listeners, hasSize(0));
		metadataStore.addListener(mockListener);
		assertThat(listeners, hasSize(1));
		assertThat(listeners, IsIterableContainingInOrder.contains(mockListener));
		metadataStore.removeListener(mockListener);
		assertThat(listeners, hasSize(0));
	}

	private void waitAtBarrier(String barrierName, Map<String, CyclicBarrier> barriers) {
		try {
			barriers.get(barrierName).await(10, TimeUnit.SECONDS);
		}
		catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
			throw new AssertionError("Test didn't complete: ", e);
		}
	}

	private CuratorFramework createNewClient() throws InterruptedException {
		CuratorFramework client = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), new BoundedExponentialBackoffRetry(100, 1000, 3));
		client.start();
		client.blockUntilConnected(10000, TimeUnit.SECONDS);
		return client;
	}

	private void closeClient(CuratorFramework client) {
		try {
			client.close();
		}
		catch (Exception e) {
			log.warn("Exception thrown while closing client: ", e);
		}
	}

	private void closeMetadataStore(ZookeeperMetadataStore metadataStore) {
		try {
			metadataStore.close();
		}
		catch (IOException e) {
			log.warn("Exception thrown while closing metadata store: ", e);
		}
	}
}
