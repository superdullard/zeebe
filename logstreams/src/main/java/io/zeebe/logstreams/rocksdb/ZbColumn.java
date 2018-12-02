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
import java.util.Map;
import java.util.Map.Entry;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public abstract class ZbColumn<K, V> implements AutoCloseable {

  /**
   * to extend the default options, see {@link
   * ColumnFamilyOptions#ColumnFamilyOptions(ColumnFamilyOptions)}
   */
  public static final ColumnFamilyOptions DEFAULT_OPTIONS =
      new ColumnFamilyOptions().optimizeUniversalStyleCompaction();

  protected final DirectBuffer keyBufferView = new UnsafeBuffer();
  protected final DirectBuffer valueBufferView = new UnsafeBuffer();

  protected final ZbRocksDb db;
  protected final ColumnFamilyHandle handle;

  public ZbColumn(ZbRocksDb db, ColumnFamilyHandle handle) {
    this.db = db;
    this.handle = handle;
  }

  @Override
  public void close() {
    handle.close();
  }

  protected abstract Serializer<K> getKeySerializer();

  protected abstract MutableDirectBuffer getKeyBuffer();

  protected abstract Serializer<V> getValueSerializer();

  protected abstract MutableDirectBuffer getValueBuffer();

  protected abstract V getValueInstance();

  public void put(K key, V value) {
    db.put(handle, serializeKey(key), serializeValue(value));
  }

  public void put(K key, V value, ZbWriteBatch batch) {
    batch.put(handle, serializeKey(key), serializeValue(value));
  }

  public void put(Map<K, V> map) {
    try (final ZbWriteBatch batch = new ZbWriteBatch()) {
      put(map, batch);
    }
  }

  public void put(Map<K, V> map, ZbWriteBatch batch) {
    for (final Entry<K, V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue(), batch);
    }

    db.write(batch);
  }

  public V get(K key) {
    final MutableDirectBuffer valueBuffer = getValueBuffer();
    final int bytesRead = db.get(handle, serializeKey(key), valueBuffer);

    if (bytesRead == RocksDB.NOT_FOUND) {
      return null;
    }

    return deserializeValue(valueBuffer, 0, bytesRead, getValueInstance());
  }

  public void delete(K key) {
    db.delete(handle, serializeKey(key));
  }

  public void delete(K key, ZbWriteBatch batch) {
    batch.delete(handle, serializeKey(key));
  }

  /**
   * Exists primarily for deleting while iterating, since we already have a pre-serialized keyBuffer
   * and don't need to serialize it again
   *
   * @param key pre-serialized key
   */
  void delete(DirectBuffer key) {
    db.delete(handle, key);
  }

  public boolean exists(K key) {
    return db.exists(handle, serializeKey(key));
  }

  // Iteration

  public ZbRocksIterator newRocksIterator() {
    return db.newIterator(handle);
  }

  public ZbRocksIterator newRocksIterator(ReadOptions options) {
    return db.newIterator(handle, options);
  }

  // Serialization/deserialization utilities

  public K deserializeKey(DirectBuffer source, int offset, int length, K instance) {
    return getKeySerializer().deserialize(source, offset, length, instance);
  }

  public DirectBuffer serializeKey(K key) {
    getKeySerializer().serialize(key, getKeyBuffer(), 0, keyBufferView);
    return keyBufferView;
  }

  public V deserializeValue(DirectBuffer source, int offset, int length, V instance) {
    return getValueSerializer().deserialize(source, offset, length, instance);
  }

  public DirectBuffer serializeValue(V value) {
    getValueSerializer().serialize(value, getValueBuffer(), 0, valueBufferView);
    return valueBufferView;
  }
}
