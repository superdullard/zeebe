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

import static io.zeebe.util.ByteArrayUtil.concat;

import io.zeebe.logstreams.rocksdb.serializers.Serializers;
import io.zeebe.logstreams.rocksdb.serializers.TupleSerializer;
import java.util.Collections;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyHandle;

public class TestState extends ZbState {

  private final TupleSerializer<Long, Integer> keySerializer =
      new TupleSerializer<>(Serializers.LONG, Serializers.INT);
  private final MutableDirectBuffer keyBuffer =
      new UnsafeBuffer(new byte[Long.BYTES + Integer.BYTES]);

  private final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();
  private final TestUnpackedObject.Serializer valueSerializer = new TestUnpackedObject.Serializer();

  private final TestColumn testColumn;

  public static List<ZbStateColumnDescriptor> getDescriptors(byte[] prefix) {
    return Collections.singletonList(
        new ZbStateColumnDescriptor<>(concat(prefix, TestColumn.NAME), TestState::newTestColumn));
  }

  public TestState(
      ZbRocksDb db,
      List<ColumnFamilyHandle> handles,
      List<ZbStateColumnDescriptor> columnDescriptors) {
    super(db, handles, columnDescriptors);

    this.testColumn = (TestColumn) columns.get(0);
  }

  public TestColumn newTestColumn(ZbRocksDb db, ColumnFamilyHandle handle) {
    return new TestColumn(db, handle, keyBuffer, keySerializer, valueBuffer, valueSerializer);
  }

  public TestColumn getTestColumn() {
    return testColumn;
  }
}
