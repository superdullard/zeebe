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

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ZbColumnIterator<K, V> implements Iterator<ZbColumnEntry<K, V>> {

  private final ZbColumnIteratorEntry<K, V> entry;
  private final ZbColumn<K, V> column;
  private final ZbRocksIterator iterator;

  private boolean hasNext;

  public ZbColumnIterator(ZbColumn<K, V> column, ZbRocksIterator iterator) {
    assert iterator.isValid() : "iterator is useless unless valid";

    this.column = column;
    this.iterator = iterator;
    this.entry = new ZbColumnIteratorEntry<>(column);
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

  @Override
  public void remove() {
    if (!hasNext) {
      throw new IllegalStateException();
    }

    column.delete(entry);
  }

  protected boolean seekNextEntry() {
    hasNext = false;

    if (iterator.isValid()) {
      entry.set(iterator.keyBuffer(), iterator.valueBuffer());
      hasNext = true;
      iterator.next();
    }

    return hasNext;
  }
}
