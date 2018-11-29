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

/**
 * Consider potentially serializing the value as a string, as it is more portable if the enum
 * constants change (e.g. order changes, or something is added/removed)
 *
 * @param <E> enum type to serialize
 */
public class EnumSerializer<E extends Enum<E>> extends AbstractSerializer<E> {

  private final E[] ordinals;

  public EnumSerializer(Class<E> enumClass) {
    this.ordinals = enumClass.getEnumConstants();
  }

  @Override
  public E newInstance() {
    return ordinals[0];
  }

  @Override
  protected int write(E value, MutableDirectBuffer dest, int offset) {
    Serializers.INT.write(value.ordinal(), dest, offset);
    return Serializers.INT.getLength();
  }

  @Override
  protected E read(DirectBuffer source, int offset, int length, E instance) {
    final int ordinal = Serializers.INT.read(source, offset, length, null);

    assert ordinal > 0 && ordinal < ordinals.length : "Serialized enum ordinal is out of bounds";
    return ordinals[ordinal];
  }

  @Override
  public int getLength() {
    return Serializers.INT.getLength();
  }
}
