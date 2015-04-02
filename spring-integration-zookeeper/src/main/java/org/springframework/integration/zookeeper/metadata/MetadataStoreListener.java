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

/**
 * A callback to be invoked whenever a value changes in the data store
 *
 * @author Marius Bogoevici
 */
public interface MetadataStoreListener {

	void onAdd(String key, String value);

	void onRemove(String key, String oldValue);

	void onUpdate(String key, String newValue);
}
