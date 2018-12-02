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
import io.zeebe.util.collection.Reusable;
import org.agrona.DirectBuffer;

public class ZbColumnEntry<K, V> implements Reusable {

  private final Serializer<K> keySerializer;
  private DirectBuffer keyBuffer;
  private K key;
  private boolean isKeySet;

  private final Serializer<V> valueSerializer;
  private DirectBuffer valueBuffer;
  private V value;
  private boolean isValueSet;

  private boolean isSet;

  public ZbColumnEntry(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public ZbColumnEntry(Serializer<K> keySerializer, K key, Serializer<V> valueSerializer) {
    this(keySerializer, valueSerializer);
    this.key = key;
  }

  public ZbColumnEntry(Serializer<K> keySerializer, Serializer<V> valueSerializer, V value) {
    this(keySerializer, valueSerializer);
    this.value = value;
  }

  public ZbColumnEntry(Serializer<K> keySerializer, K key, Serializer<V> valueSerializer, V value) {
    this(keySerializer, valueSerializer);
    this.key = key;
    this.value = value;
  }

  public boolean isSet() {
    return isSet;
  }

  @Override
  public void reset() {
    isSet = false;
    isKeySet = false;
    isValueSet = false;
  }

  public K getKey() {
    if (isKeySet) {
      return key;
    }

    isKeySet = true;
    return lazyGet(keyBuffer, keySerializer, key);
  }

  public V getValue() {
    if (isValueSet) {
      return value;
    }

    isValueSet = true;
    return lazyGet(valueBuffer, valueSerializer, value);
  }

  public void set(DirectBuffer keyBuffer, DirectBuffer valueBuffer) {
    isSet = true;
    this.keyBuffer = keyBuffer;
    this.valueBuffer = valueBuffer;
  }

  private <T> T lazyGet(DirectBuffer source, Serializer<T> serializer, T instance) {
    assert isSet() : "entry is not set, cannot deserialize anything";
    instance = serializer.deserialize(source, 0, source.capacity(), instance);

    return instance;
  }
}
