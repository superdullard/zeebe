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

import io.zeebe.logstreams.rocksdb.serializers.primitives.IntegerSerializer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public abstract class BufferSerializer<T> implements Serializer<T> {

  private static final IntegerSerializer SIZE_SERIALIZER = Serializers.INT;
  private static final int SIZE_SERIALIZER_LENGTH = SIZE_SERIALIZER.getLength();

  private final int length;

  public BufferSerializer() {
    this(VARIABLE_LENGTH);
  }

  public BufferSerializer(int length) {
    this.length = length;
  }

  protected abstract void put(
      MutableDirectBuffer dest, int offset, T value, int valueOffset, int valueLength);

  protected abstract T get(DirectBuffer source, int offset, T dest, int destOffset, int length);

  protected abstract int getSize(T buffer);

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public int serialize(T value, MutableDirectBuffer dest, int offset) {
    int bytesWritten = 0;
    int length = getLength();

    if (length == VARIABLE_LENGTH) {
      length = getSize(value);
      SIZE_SERIALIZER.serialize(length, dest, offset);
      bytesWritten += SIZE_SERIALIZER_LENGTH;
    }

    put(dest, offset + bytesWritten, value, 0, length);
    return bytesWritten + length;
  }

  @Override
  public T deserialize(DirectBuffer source, int offset, int length, T instance) {
    int bytesRead = 0;
    int serializedLength = getLength();

    if (serializedLength == VARIABLE_LENGTH) {
      serializedLength = SIZE_SERIALIZER.deserialize(source, offset, SIZE_SERIALIZER_LENGTH);
      bytesRead = SIZE_SERIALIZER_LENGTH;
    }

    get(source, offset + bytesRead, instance, 0, serializedLength);

    assert serializedLength + bytesRead == length : "Bytes read differs from expected length";
    return instance;
  }
}
