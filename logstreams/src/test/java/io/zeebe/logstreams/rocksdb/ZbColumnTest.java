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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.zeebe.logstreams.rocksdb.serializers.Serializer;
import io.zeebe.logstreams.rocksdb.serializers.Serializers;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

public class ZbColumnTest {

  private ZbRocksDb db;
  private Column column;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setup() throws IOException, RocksDBException {
    final File dbFolder = temporaryFolder.newFolder("db");
    db = ZbRocksDb.open(new Options().setCreateIfMissing(true), dbFolder.getAbsolutePath());
    column = new Column(db, db.getDefaultColumnFamily());
  }

  @After
  public void teardown() {
    column.close();
    db.close();
  }

  @Test
  public void shouldSerializeAndDeserializeKey() {
    // given
    final DirectBuffer serialized = column.serializeKey(1L);

    // when
    final long key = column.deserializeKey(serialized, 0, Long.BYTES, null);

    // then
    assertThat(key).isEqualTo(1L);
  }

  @Test
  public void shouldSerializeAndDeserializeValue() {
    // given
    final DirectBuffer serialized = column.serializeValue(false);

    // when
    final boolean value = column.deserializeValue(serialized, 0, Long.BYTES, null);

    // then
    assertThat(value).isEqualTo(false);
  }

  @Test
  public void shouldPutAndGetKeyValuePair() {
    // given
    final long key = 256L;
    final boolean value = true;

    // when
    column.put(key, value);

    // then
    assertThat(column.get(key)).isEqualTo(value);
  }

  @Test
  public void shouldReturnNullIfNoSuchKey() {
    // given
    final long key = 256L;

    // then
    assertThat(column.get(key)).isNull();
  }

  @Test
  public void shouldDeleteKey() {
    // given
    final long key = 256L;
    final boolean value = true;

    // when
    column.put(key, value);
    column.delete(key);

    // then
    assertThat(column.exists(key)).isFalse();
  }

  @Test
  public void shouldExist() {
    // given
    final long key = 256L;
    final boolean value = true;

    // when
    column.put(key, value);

    // then
    assertThat(column.exists(key)).isTrue();
  }

  @Test
  public void shouldCloseHandleOnClose() {
    // when
    column.close();

    // then
    assertThat(column.handle.isOwningHandle()).isFalse();
  }

  @Test
  public void shouldIterateOverAllKeysInOrder() {
    // given
    final SortedMap<Long, Boolean> collected = new TreeMap<>();
    final Map<Long, Boolean> data = new HashMap<>();
    data.put(1L, false);
    data.put(2L, true);
    data.put(3L, false);

    // when
    column.put(data);
    try (final ZbRocksIterator rocksIterator = column.newRocksIterator()) {
      rocksIterator.seekToFirst();
      column.iterator().reset(rocksIterator);

      for (final ZbColumnEntry<Long, Boolean> entry : column) {
        collected.put(entry.getKey(), entry.getValue());
      }
    }

    // then
    assertThat(collected).containsExactly(entry(1L, false), entry(2L, true), entry(3L, false));
  }

  class Column extends ZbColumn<Long, Boolean> implements Iterable<ZbColumnEntry<Long, Boolean>> {

    private final UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[Long.BYTES]);
    private final UnsafeBuffer valueBuffer = new UnsafeBuffer(new byte[1]);

    private final Iterator iterator = new Iterator();

    Column(ZbRocksDb db, ColumnFamilyHandle handle) {
      super(db, handle);
    }

    @Override
    protected Serializer<Long> getKeySerializer() {
      return Serializers.LONG;
    }

    @Override
    protected MutableDirectBuffer getKeyBuffer() {
      return keyBuffer;
    }

    @Override
    protected Serializer<Boolean> getValueSerializer() {
      return Serializers.BOOL;
    }

    @Override
    protected MutableDirectBuffer getValueBuffer() {
      return valueBuffer;
    }

    @Override
    protected Boolean getValueInstance() {
      return Boolean.FALSE;
    }

    @Override
    public Iterator iterator() {
      return iterator;
    }

    final class Iterator extends ZbColumnIterator<Long, Boolean> {

      private Iterator() {
        super(new ZbColumnEntry<>(getKeySerializer(), getValueSerializer()));
      }
    }
  }
}
