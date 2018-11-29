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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ListSerializerTest<T> {

  @Parameter(0)
  public String name;

  @Parameter(1)
  public List<T> list;

  @Parameter(2)
  public ListSerializer<T> serializer;

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    final List<Object[]> parameters = new ArrayList<>();

    parameters.add(
        new Object[] {
          "fixed length items", Arrays.asList(1L, 2L, 3L), new ListSerializer<>(Serializers.LONG)
        });
    parameters.add(
        new Object[] {
          "variable length items",
          Arrays.asList(wrapString("foo"), wrapString("bar"), wrapString("baz")),
          new ListSerializer<>(Serializers.DIRECT_BUFFER)
        });

    return parameters;
  }

  @Test
  public void shouldSerializeAndDeserializeList() {
    // given
    final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
    final List<Long> original = Arrays.asList(1L, 2L, 3L);
    final Serializer<List<Long>> serializer = new ListSerializer<>(Serializers.LONG);

    // when
    final DirectBuffer serialized = serializer.serialize(original, buffer);
    final List<Long> deserialized = serializer.deserialize(serialized);

    // then
    assertThat(deserialized).isEqualTo(original);
  }
}
