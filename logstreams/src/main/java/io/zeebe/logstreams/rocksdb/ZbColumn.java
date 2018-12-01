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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

public class ZbColumn<K, V> implements Iterable<ZbColumnEntry<K, V>>, AutoCloseable {

  /**
   * to extend the default options, see {@link
   * ColumnFamilyOptions#ColumnFamilyOptions(ColumnFamilyOptions)}
   */
  public static final ColumnFamilyOptions DEFAULT_OPTIONS =
      new ColumnFamilyOptions().optimizeUniversalStyleCompaction();

  protected final ZbRocksDb db;
  protected final ColumnFamilyHandle handle;

  protected final Serializer<K> keySerializer;
  protected final MutableDirectBuffer keyBuffer;
  protected final DirectBuffer keyBufferView = new UnsafeBuffer(0, 0);

  protected final Serializer<V> valueSerializer;
  protected final MutableDirectBuffer valueBuffer;
  protected final DirectBuffer valueBufferView = new UnsafeBuffer(0, 0);

  public ZbColumn(
      ZbRocksDb db,
      ColumnFamilyHandle handle,
      MutableDirectBuffer keyBuffer,
      Serializer<K> keySerializer,
      MutableDirectBuffer valueBuffer,
      Serializer<V> valueSerializer) {
    this.db = db;
    this.handle = handle;
    this.keyBuffer = keyBuffer;
    this.keySerializer = keySerializer;
    this.valueBuffer = valueBuffer;
    this.valueSerializer = valueSerializer;
  }

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
    final int bytesRead = db.get(handle, serializeKey(key), valueBuffer);

    if (bytesRead == RocksDB.NOT_FOUND) {
      return null;
    }

    return valueSerializer.deserialize(valueBuffer, 0, bytesRead, null); // TODO use instance
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
   * @param entry iterator entry to remove
   */
  void delete(ZbColumnIteratorEntry entry) {
    db.delete(handle, entry.getKeyBuffer());
  }

  public boolean exists(K key) {
    return db.exists(handle, serializeKey(key));
  }

  /** NOTE: does not close the RocksIterator once finished. */
  @Override
  public Iterator<ZbColumnEntry<K, V>> iterator() {
    final ZbRocksIterator rocksIterator = db.newIterator(handle);
    rocksIterator.seekToFirst();

    return new ZbColumnIterator<>(this, rocksIterator);
  }

  public Iterator<ZbColumnEntry<K, V>> iterator(ZbRocksIterator iterator) {
    return new ZbColumnIterator<>(this, iterator);
  }

  @Override
  public Spliterator<ZbColumnEntry<K, V>> spliterator() {
    return Spliterators.spliterator(
        iterator(),
        db.getEstimatedNumberOfKeys(handle),
        Spliterator.SORTED | Spliterator.NONNULL | Spliterator.DISTINCT);
  }

  public Stream<ZbColumnEntry<K, V>> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  public ZbRocksIterator newIterator() {
    return db.newIterator(handle);
  }

  public ZbRocksIterator newIterator(ReadOptions options) {
    return db.newIterator(handle, options);
  }

  @Override
  public void close() {
    handle.close();
  }

  public K deserializeKey(DirectBuffer source, int offset, int length) {
    return keySerializer.deserialize(source, offset, length, null); // TODO use instance
  }

  public DirectBuffer serializeKey(K key) {
    keySerializer.serialize(key, keyBuffer, 0, keyBufferView);
    return keyBufferView;
  }

  public V deserializeValue(DirectBuffer source, int offset, int length) {
    return valueSerializer.deserialize(source, offset, length, null); // TODO use instance
  }

  public DirectBuffer serializeValue(V value) {
    valueSerializer.serialize(value, valueBuffer, 0, valueBufferView);
    return valueBufferView;
  }
}
