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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 */
public class ZookeeperMetadataStore implements ListenableMetadataStore, InitializingBean, Closeable, DisposableBean {

	private final CuratorFramework client;

	private final List<MetadataStoreListener> listeners;

	private String encoding = "UTF-8";

	private PathChildrenCache cache;

	private String root = "/SI";

	/**
	 * An internal map storing local updates, ensuring that they have precedence if the cache contains stale data.
	 * As changes are propagated back from Zookeeper to the cache, entries are removed.
	 */
	private ConcurrentMap<String, LocalChildData> updateMap;

	public ZookeeperMetadataStore(CuratorFramework client) throws Exception {
		this.client = client;
		this.listeners = new CopyOnWriteArrayList<>();
		this.updateMap = new ConcurrentHashMap<>();
	}

	/**
	 * Encoding to use when storing data in ZooKeeper
	 *
	 * @param encoding encoding as text
	 */
	public void setEncoding(String encoding) {
		Assert.notNull(encoding, "'encoding' must not be null.");
		this.encoding = encoding;
	}

	/**
	 * Root node - store entries are children of this node
	 *
	 * @param root encoding as text
	 */
	public void setRoot(String root) {
		Assert.notNull(root, "'root' must not be null.");
		Assert.isTrue(root.startsWith("/"), "'root' must start with '/'");
		Assert.isTrue(!root.endsWith("/"), "'root' must not end with '/'");
		this.root = root;
	}

	public String getRoot() {
		return this.root;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.cache = new PathChildrenCache(this.client, this.root, true);
		this.cache.getListenable().addListener(new MetadataStoreListenerInvokingPathChildrenCacheListener());
		this.cache.start();
		EnsurePath ensurePath = new EnsurePath(root);
		ensurePath.ensure(client.getZookeeperClient());
	}

	@Override
	public void close() throws IOException {
		CloseableUtils.closeQuietly(this.cache);
	}

	@Override
	public void destroy() throws Exception {
		this.close();
	}


	@Override
	public String putIfAbsent(String key, String value) {
		Assert.notNull(key, "'key' must not be null.");
		Assert.notNull(value, "'value' must not be null.");
		synchronized (this.updateMap) {
			try {
				createNode(key,value);
				return null;
			}
			catch (KeeperException.NodeExistsException e) {
				// so the data actually exists, we can read it
				try {
					byte[] bytes = this.client.getData().forPath(getPath(key));
					return Conversions.bytesToString(bytes, "UTF-8");
				}
				catch (Exception exceptionDuringGet) {
					throw new ZookeeperMetadataStoreException("Exception while creating node with key '" + key + "':", e);
				}
			}
			catch (Exception e) {
				throw new ZookeeperMetadataStoreException("Error while trying to set '" + key + "':",e);
			}
		}
	}

	@Override
	public boolean replace(String key, String oldValue, String newValue) {
		Assert.notNull(key, "'key' must not be null.");
		Assert.notNull(oldValue, "'oldValue' must not be null.");
		Assert.notNull(newValue, "'newValue' must not be null.");
		synchronized (this.updateMap) {
			Stat currentStat = new Stat();
			try {
				byte[] bytes = this.client.getData().storingStatIn(currentStat).forPath(getPath(key));
				if (oldValue.equals(Conversions.bytesToString(bytes, "UTF-8"))) {
					updateNode(key,newValue,currentStat.getVersion());
				}
				return true;
			}
			catch (KeeperException.NoNodeException e) {
				// ignore, the node doesn't exist there's nothing to replace
				return false;
			}
			catch (KeeperException.BadVersionException e) {
				// ignore
				return false;
			}
			catch (Exception e) {
				throw new ZookeeperMetadataStoreException("Cannot replace value");
			}
		}
	}

	@Override
	public void addListener(MetadataStoreListener callback) {
		this.listeners.add(callback);
	}

	@Override
	public void removeListener(MetadataStoreListener callback) {
		this.listeners.remove(callback);
	}

	@Override
	public void put(String key, String value) {
		Assert.notNull(key, "'key' must not be null.");
		Assert.notNull(value, "'value' must not be null.");
		synchronized (this.updateMap) {
			try {
				Stat currentNode = this.client.checkExists().forPath(getPath(key));
				if (currentNode == null) {
					try {
						createNode(key, value);
					}
					catch (KeeperException.NodeExistsException e) {
						updateNode(key, value, -1);
					}
				}
				else {
					updateNode(key, value, -1);
				}
			}
			catch (Exception e) {
				throw new ZookeeperMetadataStoreException("Error while setting value for key '" + key + "':", e);
			}
		}
	}

	@Override
	public String get(String key) {
		Assert.notNull(key, "'key' must not be null.");
		synchronized (this.updateMap) {
			ChildData currentData = this.cache.getCurrentData(getPath(key));
			if (currentData == null) {
				if (this.updateMap.containsKey(key)) {
					// we have saved the value, but the cache hasn't updated yet
					// if the value had changed via replication, we would have been notified by the listener
					return this.updateMap.get(key).getValue();
				}
				else {
					// the value just doesn't exist
					return null;
				}
			}
			else {
				if (this.updateMap.containsKey(key)) {
					// our version is more recent than the cache
					if (this.updateMap.get(key).getVersion() >= currentData.getStat().getVersion()) {
						return this.updateMap.get(key).getValue();
					}
				}
				return Conversions.bytesToString(currentData.getData(), "UTF-8");
			}
		}
	}

	@Override
	public String remove(String key) {
		Assert.notNull(key, "'key' must not be null.");
		synchronized (this.updateMap) {
			try {
				byte[] bytes = this.client.getData().forPath(getPath(key));
				this.client.delete().forPath(getPath(key));
				// we guarantee that the deletion will supersede the existing data
				this.updateMap.put(key,new LocalChildData(null,Integer.MAX_VALUE));
				return Conversions.bytesToString(bytes, "UTF-8");
			}
			catch (KeeperException.NoNodeException e) {
				// ignore - the node doesn't exist
				return null;
			}
			catch (Exception e) {
				throw new ZookeeperMetadataStoreException("Exception while deleting key '" + key + "'", e);
			}
		}
	}

	private void updateNode(String key, String value, int version) throws Exception {
		Stat stat = this.client.setData().withVersion(version).forPath(getPath(key),
				Conversions.stringToBytes(value, this.encoding));
		this.updateMap.put(key, new LocalChildData(value, stat.getVersion()));
	}

	private void createNode(String key, String value) throws Exception {
		this.client.create().forPath(getPath(key), Conversions.stringToBytes(value, this.encoding));
		this.updateMap.put(key, new LocalChildData(value, 0));
	}

	public String getPath(String key) {
		return "".equals(key) ? this.root : this.root + "/" + key;
	}

	private static class LocalChildData {

		String value;

		int version;

		public LocalChildData(String value, int version) {
			this.value = value;
			this.version = version;
		}

		public String getValue() {
			return this.value;
		}

		public int getVersion() {
			return this.version;
		}
	}

	private class MetadataStoreListenerInvokingPathChildrenCacheListener implements PathChildrenCacheListener {
		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			synchronized (ZookeeperMetadataStore.this.updateMap) {
				switch (event.getType()) {
					case CHILD_ADDED:
						if (ZookeeperMetadataStore.this.updateMap.containsKey(getKey(event.getData().getPath()))) {
							if (event.getData().getStat().getVersion() >=
									ZookeeperMetadataStore.this.updateMap.get(getKey(event.getData().getPath())).getVersion()) {
								ZookeeperMetadataStore.this.updateMap.remove(event.getData().getPath());
							}
						}
						for (MetadataStoreListener listener : ZookeeperMetadataStore.this.listeners) {
							listener.onAdd(getKey(event.getData().getPath()), Conversions.bytesToString(event.getData().getData(), "UTF-8"));
						}
						break;
					case CHILD_UPDATED:
						if (ZookeeperMetadataStore.this.updateMap.containsKey(getKey(event.getData().getPath()))) {
							if (event.getData().getStat().getVersion() >=
									ZookeeperMetadataStore.this.updateMap.get(getKey(event.getData().getPath())).getVersion()) {
								ZookeeperMetadataStore.this.updateMap.remove(event.getData().getPath());
							}
						}
						for (MetadataStoreListener listener : ZookeeperMetadataStore.this.listeners) {
							listener.onUpdate(getKey(event.getData().getPath()), Conversions.bytesToString(event.getData().getData(), "UTF-8"));
						}
						break;
					case CHILD_REMOVED:
						ZookeeperMetadataStore.this.updateMap.remove(getKey(event.getData().getPath()));
						for (MetadataStoreListener listener : ZookeeperMetadataStore.this.listeners) {
							listener.onRemove(getKey(event.getData().getPath()), Conversions.bytesToString(event.getData().getData(), "UTF-8"));
						}
						break;
					default:
						// ignore all other events
						break;
				}
			}
		}
	}

	private String getKey(String path) {
		return path.replace(root + "/", "");
	}
}
