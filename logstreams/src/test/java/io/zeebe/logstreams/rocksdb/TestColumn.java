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
import org.rocksdb.ColumnFamilyHandle;

public class TestColumn extends ZbColumn<Tuple<Long, Integer>, TestUnpackedObject> {
  public static final byte[] NAME = "test".getBytes();

  private final TestUnpackedObject valueInstance = new TestUnpackedObject();
  private final TupleSerializer<Long, Integer> keySerializer;
  private final MutableDirectBuffer keyBuffer;
  private final Serializer<TestUnpackedObject> valueSerializer;
  private final MutableDirectBuffer valueBuffer;

  public TestColumn(
      ZbRocksDb db,
      ColumnFamilyHandle columnFamilyHandle,
      MutableDirectBuffer keyBuffer,
      TupleSerializer<Long, Integer> keySerializer,
      MutableDirectBuffer valueBuffer,
      Serializer<TestUnpackedObject> valueSerializer) {
    super(db, columnFamilyHandle);
    this.keySerializer = keySerializer;
    this.keyBuffer = keyBuffer;
    this.valueSerializer = valueSerializer;
    this.valueBuffer = valueBuffer;
  }

  @Override
  protected TupleSerializer<Long, Integer> getKeySerializer() {
    return keySerializer;
  }

  @Override
  protected MutableDirectBuffer getKeyBuffer() {
    return keyBuffer;
  }

  @Override
  protected Serializer<TestUnpackedObject> getValueSerializer() {
    return valueSerializer;
  }

  @Override
  protected MutableDirectBuffer getValueBuffer() {
    return valueBuffer;
  }

  @Override
  protected TestUnpackedObject getValueInstance() {
    return valueInstance;
  }
}
