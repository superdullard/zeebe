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
public class PrimitiveSerializer<T> extends AbstractSerializer<T> {

  private final Getter<T> getter;
  private final Putter<T> putter;
  private final int length;
  private final T instance;

  public PrimitiveSerializer(Getter<T> getter, Putter<T> putter, int length, T instance) {
    this.getter = getter;
    this.putter = putter;
    this.length = length;
    this.instance = instance;
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
    putter.put(dest, offset, value, STATE_BYTE_ORDER);
    return getLength();
  }

  @Override
  protected T read(DirectBuffer source, int offset, int length, T instance) {
    return getter.get(source, offset, STATE_BYTE_ORDER);
  }

  @FunctionalInterface
  interface Getter<P> {

    P get(DirectBuffer source, int offset, ByteOrder byteOrder);
  }

  @FunctionalInterface
  interface Putter<P> {

    void put(MutableDirectBuffer dest, int offset, P value, ByteOrder byteOrder);
  }
}
