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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class BufferSerializer<T> extends AbstractSerializer<T> {

  private static final PrimitiveSerializer<Integer> SIZE_SERIALIZER = Serializers.INT;
  private static final int SIZE_SERIALIZER_LENGTH = SIZE_SERIALIZER.getLength();

  private final Copier<T> getter;
  private final Putter<T> putter;
  private final Sizer<T> sizer;
  private final int length;
  private final T instance;

  public BufferSerializer(
      Copier<T> getter, Putter<T> putter, Sizer<T> sizer, boolean hasFixedLength, T instance) {
    this.getter = getter;
    this.putter = putter;
    this.sizer = sizer;
    this.instance = instance;
    this.length = hasFixedLength ? sizer.size(instance) : VARIABLE_LENGTH;
  }

  @Override
  public T newInstance() {
    return instance;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  protected int write(T value, MutableDirectBuffer dest, int offset) {
    int bytesWritten = 0;
    int length = getLength();

    if (length == VARIABLE_LENGTH) {
      length = sizer.size(value);
      SIZE_SERIALIZER.serialize(length, dest, offset);
      bytesWritten += SIZE_SERIALIZER_LENGTH;
    }

    putter.put(dest, offset + bytesWritten, value, 0, length);
    return bytesWritten + length;
  }

  @Override
  protected T read(DirectBuffer source, int offset, int length, T instance) {
    int bytesRead = 0;
    int serializedLength = getLength();

    if (serializedLength == VARIABLE_LENGTH) {
      serializedLength = SIZE_SERIALIZER.deserialize(source, offset, SIZE_SERIALIZER_LENGTH);
      bytesRead = SIZE_SERIALIZER_LENGTH;
    }

    getter.copy(source, offset + bytesRead, instance, 0, serializedLength);

    assert serializedLength + bytesRead == length : "Bytes read differs from expected length";
    return instance;
  }

  @FunctionalInterface
  interface Copier<P> {

    void copy(DirectBuffer source, int offset, P dest, int destOffset, int length);
  }

  @FunctionalInterface
  interface Putter<P> {

    void put(MutableDirectBuffer dest, int offset, P value, int valueOffset, int valueLength);
  }

  @FunctionalInterface
  interface Sizer<T> {

    int size(T value);
  }
}
