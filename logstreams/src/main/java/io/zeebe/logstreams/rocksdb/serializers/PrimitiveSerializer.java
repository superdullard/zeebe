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

import static io.zeebe.logstreams.rocksdb.ZeebeStateConstants.STATE_BYTE_ORDER;

import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * One small caveat: using these static instances means they are not thread safe, since they have
 * very minimal state: the bufferView.
 */
public abstract class PrimitiveSerializer<T> implements Serializer<T> {

  private final int length;

  public PrimitiveSerializer(int length) {
    this.length = length;
  }

  protected abstract void put(MutableDirectBuffer dest, int offset, T value, ByteOrder byteOrder);

  protected abstract T get(DirectBuffer source, int offset, ByteOrder byteOrder);

  @Override
  public int getLength() {
    return length;
  }

  public int serialize(T value, MutableDirectBuffer dest, int offset) {
    put(dest, offset, value, STATE_BYTE_ORDER);
    return getLength();
  }

  @Override
  public T deserialize(DirectBuffer source, int offset, int length, T instance) {
    return get(source, offset, STATE_BYTE_ORDER);
  }

  public T deserialize(DirectBuffer source, int offset, int length) {
    return deserialize(source, offset, length, null);
  }
}
