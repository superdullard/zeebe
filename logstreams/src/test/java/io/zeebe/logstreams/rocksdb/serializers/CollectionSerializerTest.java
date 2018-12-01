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

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.logstreams.rocksdb.serializers.collections.ArraySerializer;
import io.zeebe.logstreams.rocksdb.serializers.collections.ListSerializer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CollectionSerializerTest<T, T_ITEM> {

  @Parameter(0)
  public String name;

  @Parameter(1)
  public T collection;

  @Parameter(2)
  public T instance;

  @Parameter(3)
  public CollectionSerializer<T, T_ITEM> serializer;

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    final List<Object[]> parameters = new ArrayList<>();

    parameters.add(
        new Object[] {
          "list with fixed length items",
          Arrays.asList(1L, 2L, 3L),
          new ArrayList<>(),
          new ListSerializer<>(Serializers.LONG, (l, i) -> 0L)
        });
    parameters.add(
        new Object[] {
          "list with variable length items",
          Arrays.asList(wrapString("foo"), wrapString("bar"), wrapString("baz")),
          new ArrayList<>(),
          new ListSerializer<>(Serializers.BUFFER_WRAP, (l, i) -> new UnsafeBuffer())
        });

    parameters.add(
        new Object[] {
          "array with fixed length items",
          new Long[] {1L, 2L, 3L},
          new Long[3],
          new ArraySerializer<>(Serializers.LONG)
        });
    parameters.add(
        new Object[] {
          "array with variable length items",
          new DirectBuffer[] {wrapString("foo"), wrapString("bar"), wrapString("baz")},
          new DirectBuffer[] {new UnsafeBuffer(), new UnsafeBuffer(), new UnsafeBuffer()},
          new ArraySerializer<>(Serializers.BUFFER_WRAP)
        });

    parameters.add(
        new Object[] {
          "array with custom supplier",
          new DirectBuffer[] {wrapString("foo"), wrapString("bar"), wrapString("baz")},
          new DirectBuffer[3],
          new ArraySerializer<>(Serializers.BUFFER_WRAP, (l, i) -> new UnsafeBuffer())
        });

    return parameters;
  }

  @Test
  public void shouldSerializeAndDeserializeList() {
    // given
    final MutableDirectBuffer buffer = new ExpandableArrayBuffer();

    // when
    final int length = serializer.serialize(collection, buffer, 0);
    final T deserialized = serializer.deserialize(buffer, 0, length, instance);

    // then
    assertThat(deserialized).isEqualTo(collection);
  }
}
