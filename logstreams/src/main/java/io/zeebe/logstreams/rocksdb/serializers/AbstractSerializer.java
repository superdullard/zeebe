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
import org.agrona.concurrent.UnsafeBuffer;

public abstract class AbstractSerializer<T> implements Serializer<T> {

  protected final DirectBuffer bufferView;

  public AbstractSerializer() {
    this(new UnsafeBuffer(0, 0));
  }

  public AbstractSerializer(DirectBuffer bufferView) {
    this.bufferView = bufferView;
  }

  @Override
  public int getLength() {
    return VARIABLE_LENGTH;
  }

  public DirectBuffer serialize(T value, MutableDirectBuffer dest, int offset) {
    final int length = write(value, dest, offset);
    bufferView.wrap(dest, offset, length);
    return bufferView;
  }

  protected abstract int write(T value, MutableDirectBuffer dest, int offset);

  @Override
  public T deserialize(DirectBuffer source, int offset, int length) {
    final T instance = newInstance();
    return read(source, offset, length, instance);
  }

  protected abstract T read(DirectBuffer source, int offset, int length, T instance);
}
