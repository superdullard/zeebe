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
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class IntegerSerializer extends PrimitiveSerializer<Integer> {

  public IntegerSerializer() {
    super(Integer.BYTES);
  }

  @Override
  protected void put(MutableDirectBuffer dest, int offset, Integer value, ByteOrder byteOrder) {
    dest.putInt(offset, value, byteOrder);
  }

  @Override
  protected Integer get(DirectBuffer source, int offset, ByteOrder byteOrder) {
    return source.getInt(offset, byteOrder);
  }
}
