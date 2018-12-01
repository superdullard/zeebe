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

import org.agrona.DirectBuffer;

public class ZbColumnEntry<K, V> {

  private final ZbColumn<K, V> column;

  private K key;
  private V value;

  public ZbColumnEntry(ZbColumn<K, V> column) {
    this.column = column;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public void set(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public void setKey(DirectBuffer keyBuffer, int offset, int length) {
    key = column.deserializeKey(keyBuffer, offset, length); // TODO use instance
  }

  public void setValue(DirectBuffer valueBuffer, int offset, int length) {
    value = column.deserializeValue(valueBuffer, offset, length); // TODO use instance
  }
}
