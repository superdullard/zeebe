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

import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class ListSerializer<T> extends AbstractSerializer<List<T>> {

  private static final PrimitiveSerializer<Integer> SIZE_SERIALIZER = Serializers.INT;
  private static final int SIZE_SERIALIZER_LENGTH = SIZE_SERIALIZER.getLength();

  private final List<T> instance = new ArrayList<>(0);
  private final Serializer<T> elementSerializer;

  public ListSerializer(Serializer<T> elementSerializer) {
    this.elementSerializer = elementSerializer;
  }

  @Override
  public List<T> newInstance() {
    return instance;
  }

  @Override
  public int getLength() {
    return VARIABLE_LENGTH;
  }

  @Override
  protected int write(List<T> value, MutableDirectBuffer dest, int offset) {
    SIZE_SERIALIZER.serialize(value.size(), dest, offset);
    int bytesWritten = SIZE_SERIALIZER_LENGTH;

    for (final T element : value) {
      bytesWritten += writeElement(dest, offset + bytesWritten, element);
    }

    return bytesWritten;
  }

  private int writeElement(MutableDirectBuffer dest, int offset, T element) {
    final boolean hasVariableLength = elementSerializer.getLength() == VARIABLE_LENGTH;
    final int bytesWritten = hasVariableLength ? SIZE_SERIALIZER_LENGTH : 0;
    final DirectBuffer serialized =
        elementSerializer.serialize(element, dest, offset + bytesWritten);

    if (hasVariableLength) {
      SIZE_SERIALIZER.serialize(serialized.capacity(), dest, offset);
    }

    return bytesWritten + serialized.capacity();
  }

  @Override
  protected List<T> read(DirectBuffer source, int offset, int length, List<T> instance) {
    final int size = SIZE_SERIALIZER.deserialize(source, offset);
    int bytesRead = SIZE_SERIALIZER_LENGTH;

    for (int i = 0; i < size; i++) {
      bytesRead += readElement(source, instance, offset + bytesRead);
    }

    assert bytesRead == length : "Bytes read differs from length";
    return instance;
  }

  private int readElement(DirectBuffer source, List<T> instance, int offset) {
    final boolean hasVariableLength = elementSerializer.getLength() == VARIABLE_LENGTH;
    final int bytesRead;
    final T element;

    if (hasVariableLength) {
      final int elementLength = SIZE_SERIALIZER.deserialize(source, offset, SIZE_SERIALIZER_LENGTH);
      element =
          elementSerializer.deserialize(source, offset + SIZE_SERIALIZER_LENGTH, elementLength);
      bytesRead = elementLength + SIZE_SERIALIZER_LENGTH;
    } else {
      element = elementSerializer.deserialize(source, offset, elementSerializer.getLength());
      bytesRead = elementSerializer.getLength();
    }

    instance.add(element);
    return bytesRead;
  }
}
