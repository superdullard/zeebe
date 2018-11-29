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
package io.zeebe.logstreams.rocksdb.serializers;

import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class Serializers {

  // Primitives
  public static final BooleanSerializer BOOL = new BooleanSerializer();
  public static final ByteSerializer BYTE = new ByteSerializer();
  public static final PrimitiveSerializer<Character> CHAR =
      new PrimitiveSerializer<>(
          DirectBuffer::getChar, MutableDirectBuffer::putChar, Character.BYTES, (char) 0);
  public static final PrimitiveSerializer<Double> DOUBLE =
      new PrimitiveSerializer<>(
          DirectBuffer::getDouble, MutableDirectBuffer::putDouble, Double.BYTES, 0.0);
  public static final PrimitiveSerializer<Float> FLOAT =
      new PrimitiveSerializer<>(
          DirectBuffer::getFloat, MutableDirectBuffer::putFloat, Float.BYTES, 0.0f);
  public static final PrimitiveSerializer<Integer> INT =
      new PrimitiveSerializer<>(
          DirectBuffer::getInt, MutableDirectBuffer::putInt, Integer.BYTES, 0);
  public static final PrimitiveSerializer<Long> LONG =
      new PrimitiveSerializer<>(
          DirectBuffer::getLong, MutableDirectBuffer::putLong, Long.BYTES, 0L);
  public static final PrimitiveSerializer<Short> SHORT =
      new PrimitiveSerializer<>(
          DirectBuffer::getShort, MutableDirectBuffer::putShort, Short.BYTES, (short) 0);

  // Buffers

  public static final BufferSerializer<DirectBuffer> DIRECT_BUFFER =
      new BufferSerializer<>(
          Serializers::wrapAsCopy,
          Serializers::wrapAsCopy,
          DirectBuffer::capacity,
          false,
          new UnsafeBuffer(0, 0));

  private static void wrapAsCopy(
      DirectBuffer dest, int offset, DirectBuffer value, int valueOffset, int valueLength) {
    value.wrap(dest, offset, valueLength);
  }

  public static <T extends MutableDirectBuffer> BufferSerializer<T> newMutableBuffer(T buffer) {
    return new BufferSerializer<>(
        DirectBuffer::getBytes,
        MutableDirectBuffer::putBytes,
        MutableDirectBuffer::capacity,
        !buffer.isExpandable(),
        buffer);
  }

  public static BufferSerializer<ByteBuffer> newByteBuffer(ByteBuffer buffer) {
    return new BufferSerializer<>(
        DirectBuffer::getBytes, MutableDirectBuffer::putBytes, ByteBuffer::limit, true, buffer);
  }

  public static BufferSerializer<byte[]> newByteArray(byte[] buffer) {
    return new BufferSerializer<>(
        DirectBuffer::getBytes, MutableDirectBuffer::putBytes, b -> b.length, true, buffer);
  }
}
