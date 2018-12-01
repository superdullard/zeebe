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
package io.zeebe.logstreams.rocksdb.serializers.primitives;

import io.zeebe.logstreams.rocksdb.serializers.PrimitiveSerializer;
import io.zeebe.logstreams.rocksdb.serializers.Serializers;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class BooleanSerializer extends PrimitiveSerializer<Boolean> {

  private static final byte FALSE = 0;
  private static final byte TRUE = 1;

  public BooleanSerializer() {
    super(1);
  }

  @Override
  public int getLength() {
    return Serializers.BYTE.getLength();
  }

  @Override
  protected void put(MutableDirectBuffer dest, int offset, Boolean value, ByteOrder byteOrder) {
    final byte toWrite = value ? TRUE : FALSE;
    Serializers.BYTE.serialize(toWrite, dest, offset);
  }

  @Override
  protected Boolean get(DirectBuffer source, int offset, ByteOrder byteOrder) {
    final byte value = Serializers.BYTE.deserialize(source, offset, 1);

    switch (value) {
      case FALSE:
        return false;
      case TRUE:
        return true;
      default:
        throw new IllegalStateException(String.format("Unexpected serialized boolean %d", value));
    }
  }
}
