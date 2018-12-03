/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.logstreams.rocksdb;

import io.zeebe.logstreams.rocksdb.serializers.Serializer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ZbColumnIterator<K, V> implements Iterator<ZbColumnEntry<K, V>> {

  private final ZbColumnEntry<K, V> entry;

  private ZbRocksIterator rocksIterator;
  private boolean hasNext;

  public ZbColumnIterator(ZbColumnEntry<K, V> entry) {
    this.entry = entry;
  }

  public void reset(ZbRocksIterator rocksIterator) {
    this.rocksIterator = rocksIterator;
  }

  @Override
  public boolean hasNext() {
    return hasNext || seekNextEntry();
  }

  @Override
  public ZbColumnEntry<K, V> next() {
    if (hasNext || seekNextEntry()) {
      hasNext = false;
      return entry;
    }

    throw new NoSuchElementException();
  }

  protected boolean seekNextEntry() {
    assert rocksIterator != null : "no rocks iterator given";

    hasNext = false;
    entry.reset();

    if (rocksIterator.isValid()) {
      entry.set(rocksIterator.keyBuffer(), rocksIterator.valueBuffer());
      hasNext = true;
      rocksIterator.next();
    }

    return hasNext;
  }

  /**
   * Primarily used when iterating over primitives, where you don't need to supply a pre-allocated
   * instance.
   */
  public static <K, V> ZbColumnIterator<K, V> of(
      Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    return new ZbColumnIterator<>(new ZbColumnEntry<>(keySerializer, valueSerializer));
  }
}
