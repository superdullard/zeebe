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

public abstract class CollectionSerializer<T, T_ITEM> implements Serializer<T> {

  private static final IntegerSerializer SIZE_SERIALIZER = Serializers.INT;
  private static final int SIZE_SERIALIZER_LENGTH = SIZE_SERIALIZER.getLength();

  private final Serializer<T_ITEM> itemSerializer;
  private final ItemInstanceSupplier<T, T_ITEM> itemSupplier;

  public CollectionSerializer(
      Serializer<T_ITEM> itemSerializer, ItemInstanceSupplier<T, T_ITEM> itemSupplier) {
    this.itemSerializer = itemSerializer;
    this.itemSupplier = itemSupplier;
  }

  protected abstract int getSize(T collection);

  protected abstract void setItem(T collection, int index, T_ITEM item);

  protected abstract T_ITEM getItem(T collection, int index);

  @Override
  public int getLength() {
    return VARIABLE_LENGTH;
  }

  @Override
  public int serialize(T value, MutableDirectBuffer dest, int offset) {
    final int size = getSize(value);
    SIZE_SERIALIZER.serialize(size, dest, offset);
    int bytesWritten = SIZE_SERIALIZER_LENGTH;

    for (int i = 0; i < size; i++) {
      final T_ITEM item = getItem(value, i);
      bytesWritten += serializeItem(dest, offset + bytesWritten, item);
    }

    return bytesWritten;
  }

  @Override
  public T deserialize(DirectBuffer source, int offset, int length, T instance) {
    final int size = SIZE_SERIALIZER.deserialize(source, offset, SIZE_SERIALIZER_LENGTH);
    int bytesRead = SIZE_SERIALIZER_LENGTH;

    for (int i = 0; i < size; i++) {
      bytesRead += deserializeItem(source, offset + bytesRead, instance, i);
    }

    assert bytesRead == length : "Bytes read differs from length";
    return instance;
  }

  protected int serializeItem(MutableDirectBuffer dest, int offset, T_ITEM element) {
    final boolean hasVariableLength = itemSerializer.getLength() == VARIABLE_LENGTH;
    final int bytesWritten = hasVariableLength ? SIZE_SERIALIZER_LENGTH : 0;
    final int length = itemSerializer.serialize(element, dest, offset + bytesWritten);

    if (hasVariableLength) {
      SIZE_SERIALIZER.serialize(length, dest, offset);
    }

    return bytesWritten + length;
  }

  protected int deserializeItem(DirectBuffer source, int offset, T instance, int instanceOffset) {
    final boolean hasVariableLength = itemSerializer.getLength() == VARIABLE_LENGTH;
    final int bytesRead;
    T_ITEM item = itemSupplier.get(instance, instanceOffset);

    if (hasVariableLength) {
      final int itemLength = SIZE_SERIALIZER.deserialize(source, offset, SIZE_SERIALIZER_LENGTH);
      item = itemSerializer.deserialize(source, offset + SIZE_SERIALIZER_LENGTH, itemLength, item);
      bytesRead = itemLength + SIZE_SERIALIZER_LENGTH;
    } else {
      item = itemSerializer.deserialize(source, offset, itemSerializer.getLength(), item);
      bytesRead = itemSerializer.getLength();
    }

    setItem(instance, instanceOffset, item);
    return bytesRead;
  }

  @FunctionalInterface
  public interface ItemInstanceSupplier<T, T_ITEM> {

    T_ITEM get(T instance, int index);
  }
}
