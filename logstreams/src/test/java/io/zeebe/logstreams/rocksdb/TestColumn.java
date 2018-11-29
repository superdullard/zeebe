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
import io.zeebe.logstreams.rocksdb.serializers.TupleSerializer;
import io.zeebe.util.collection.Tuple;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;

public class TestColumn extends ZbColumn<Tuple<Long, Integer>, TestUnpackedObject> {
  public static final byte[] NAME = "test".getBytes();

  private final MutableDirectBuffer prefixBuffer = new UnsafeBuffer(new byte[Long.BYTES]);
  private final TupleSerializer<Long, Integer> keySerializer;

  public TestColumn(
      ZbRocksDb db,
      ColumnFamilyHandle columnFamilyHandle,
      MutableDirectBuffer keyBuffer,
      TupleSerializer<Long, Integer> keySerializer,
      MutableDirectBuffer valueBuffer,
      Serializer<TestUnpackedObject> valueSerializer) {
    super(db, columnFamilyHandle, keyBuffer, keySerializer, valueBuffer, valueSerializer);
    this.keySerializer = keySerializer;
  }

  public void forEachWithPrefix(long prefix, Visitor visitor) {
    keySerializer.serializePrefix(prefix, prefixBuffer, 0);
    try (final ReadOptions options = new ReadOptions();
        final ZbRocksIterator rocksIterator = newIterator(options)) {}
  }

  @FunctionalInterface
  public interface Visitor {
    void visit(Tuple<Long, Integer> key, TestUnpackedObject value);
  }
}
