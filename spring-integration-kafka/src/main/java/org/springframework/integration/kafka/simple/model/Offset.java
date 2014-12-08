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


package org.springframework.integration.kafka.simple.model;

/**
 * @author Marius Bogoevici
 */
public class Offset {

	private Partition partition;

	private long offset;

	public Offset(Partition partition, long offset) {
		this.partition = partition;
		this.offset = offset;
	}

	public Partition getPartition() {
		return partition;
	}

	public void setPartition(Partition partition) {
		this.partition = partition;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Offset offset1 = (Offset) o;

		if (offset != offset1.offset) return false;
		if (!partition.equals(offset1.partition)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = partition.hashCode();
		result = 31 * result + (int) (offset ^ (offset >>> 32));
		return result;
	}
}
