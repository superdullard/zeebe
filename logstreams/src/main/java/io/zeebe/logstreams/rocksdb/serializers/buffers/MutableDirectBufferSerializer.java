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
package io.zeebe.logstreams.rocksdb.serializers.buffers;

import io.zeebe.logstreams.rocksdb.serializers.BufferSerializer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class MutableDirectBufferSerializer extends BufferSerializer<MutableDirectBuffer> {

  public MutableDirectBufferSerializer() {}

  public MutableDirectBufferSerializer(int length) {
    super(length);
  }

  @Override
  protected void put(
      MutableDirectBuffer dest,
      int offset,
      MutableDirectBuffer value,
      int valueOffset,
      int valueLength) {
    dest.putBytes(offset, value, valueOffset, valueLength);
  }

  @Override
  protected MutableDirectBuffer get(
      DirectBuffer source, int offset, MutableDirectBuffer dest, int destOffset, int length) {
    dest.putBytes(destOffset, source, offset, length);
    return dest;
  }

  @Override
  protected int getSize(MutableDirectBuffer buffer) {
    return buffer.capacity();
  }
}
