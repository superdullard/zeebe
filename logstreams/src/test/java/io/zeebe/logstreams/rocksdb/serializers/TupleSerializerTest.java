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

import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.collection.Tuple;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Test;

public class TupleSerializerTest {

  @Test
  public void shouldSerializeAndDeserialize() {
    // given
    final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
    final Tuple<Long, DirectBuffer> original =
        new Tuple<>(1L, BufferUtil.wrapString("Max Mustermann"));
    final Serializer<Tuple<Long, DirectBuffer>> serializer =
        new TupleSerializer<>(Serializers.LONG, Serializers.DIRECT_BUFFER);

    // when
    final DirectBuffer serialized = serializer.serialize(original, buffer);
    final Tuple<Long, DirectBuffer> deserialized = serializer.deserialize(serialized);

    // then
    assertThat(deserialized).isEqualToComparingOnlyGivenFields(original, "left", "right");
  }

  @Test
  public void shouldSerializePrefix() {
    // given
    final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
    final Tuple<Long, DirectBuffer> data = new Tuple<>(1L, BufferUtil.wrapString("Max Mustermann"));
    final TupleSerializer<Long, DirectBuffer> serializer =
        new TupleSerializer<>(Serializers.LONG, Serializers.DIRECT_BUFFER);

    // when
    final DirectBuffer serialized = serializer.serialize(data, buffer, 0);
    final DirectBuffer prefix = serializer.serializePrefix(1L, buffer, serialized.capacity());

    // then
    assertThat(BufferUtil.startsWith(serialized, prefix)).isTrue();
  }
}
