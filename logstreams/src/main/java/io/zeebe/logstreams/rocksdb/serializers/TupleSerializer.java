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

import io.zeebe.util.collection.Tuple;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class TupleSerializer<L, R> extends AbstractSerializer<Tuple<L, R>>
    implements PrefixSerializer<L> {

  private static final PrimitiveSerializer<Integer> SIZE_SERIALIZER = Serializers.INT;
  private static final int SIZE_SERIALIZER_LENGTH = SIZE_SERIALIZER.getLength();

  private final Tuple<L, R> instance = new Tuple<>();
  private final Serializer<L> leftSerializer;
  private final Serializer<R> rightSerializer;

  public TupleSerializer(Serializer<L> leftSerializer, Serializer<R> rightSerializer) {
    this.leftSerializer = leftSerializer;
    this.rightSerializer = rightSerializer;
  }

  @Override
  public Tuple<L, R> newInstance() {
    return instance;
  }

  @Override
  public int getLength() {
    final int leftLength = leftSerializer.getLength();
    final int rightLength = rightSerializer.getLength();

    if (leftLength == VARIABLE_LENGTH || rightLength == VARIABLE_LENGTH) {
      return VARIABLE_LENGTH;
    }

    return leftLength + rightLength;
  }

  @Override
  protected int write(Tuple<L, R> value, MutableDirectBuffer dest, int offset) {
    int bytesWritten = writePart(value.getLeft(), dest, offset, leftSerializer);
    bytesWritten += writePart(value.getRight(), dest, offset + bytesWritten, rightSerializer);

    assert getLength() == VARIABLE_LENGTH || bytesWritten == getLength()
        : "Bytes written differs from expected length";
    return bytesWritten;
  }

  @Override
  protected Tuple<L, R> read(DirectBuffer source, int offset, int length, Tuple<L, R> instance) {
    int bytesRead = readPart(instance::setLeft, source, offset, leftSerializer);
    bytesRead += readPart(instance::setRight, source, offset + bytesRead, rightSerializer);

    assert bytesRead == length : "Bytes read differs from length";
    return instance;
  }

  @Override
  public Serializer<L> getPrefixSerializer() {
    return leftSerializer;
  }

  private <T> int writePart(
      T value, MutableDirectBuffer dest, int offset, Serializer<T> serializer) {
    int bytesWritten = serializer.getLength();

    if (bytesWritten != VARIABLE_LENGTH) {
      serializer.serialize(value, dest, offset);
    } else {
      final DirectBuffer view = serializer.serialize(value, dest, offset + SIZE_SERIALIZER_LENGTH);
      SIZE_SERIALIZER.serialize(view.capacity(), dest, offset);
      bytesWritten = SIZE_SERIALIZER_LENGTH + view.capacity();
    }

    return bytesWritten;
  }

  private <T> int readPart(
      Consumer<T> setter, DirectBuffer source, int offset, Serializer<T> serializer) {
    final int bytesRead;
    int length = serializer.getLength();

    if (length == VARIABLE_LENGTH) {
      length = SIZE_SERIALIZER.deserialize(source, offset, SIZE_SERIALIZER_LENGTH);
      offset += SIZE_SERIALIZER_LENGTH;
      bytesRead = length + SIZE_SERIALIZER_LENGTH;
    } else {
      bytesRead = length;
    }

    final T value = serializer.deserialize(source, offset, length);
    setter.accept(value);

    return bytesRead;
  }
}
