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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PrimitiveSerializerTest<T> {

  @Parameter(0)
  public String name;

  @Parameter(1)
  public T value;

  @Parameter(2)
  public Serializer<T> serializer;

  @Parameter(3)
  public int length;

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    final List<Object[]> parameters = new ArrayList<>();
    parameters.add(new Object[] {"bool", random.nextBoolean(), Serializers.BOOL, 1});
    parameters.add(
        new Object[] {
          "byte", (byte) random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE), Serializers.BYTE, 1
        });
    parameters.add(
        new Object[] {
          "char",
          (char) random.nextInt(Character.MIN_VALUE, Character.MAX_VALUE),
          Serializers.CHAR,
          Character.BYTES
        });
    parameters.add(new Object[] {"double", random.nextDouble(), Serializers.DOUBLE, Double.BYTES});
    parameters.add(new Object[] {"float", random.nextFloat(), Serializers.FLOAT, Float.BYTES});
    parameters.add(new Object[] {"int", random.nextInt(), Serializers.INT, Integer.BYTES});
    parameters.add(new Object[] {"long", random.nextLong(), Serializers.LONG, Long.BYTES});
    parameters.add(
        new Object[] {
          "short",
          (short) random.nextInt(Short.MIN_VALUE, Short.MAX_VALUE),
          Serializers.SHORT,
          Short.BYTES
        });

    return parameters;
  }

  @Test
  public void shouldReturnExpectedLength() {
    // then
    assertThat(serializer.getLength()).isEqualTo(length);
  }

  @Test
  public void shouldSerializeAndDeserializePrimitive() {
    // given
    final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[serializer.getLength()]);

    // when
    final DirectBuffer serialized = serializer.serialize(value, buffer, 0);
    final T deserialized = serializer.deserialize(serialized, 0, serialized.capacity());

    // then
    assertThat(deserialized).isEqualTo(value);
  }
}
