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

import io.zeebe.util.buffer.BufferReader;
import io.zeebe.util.buffer.BufferWriter;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Test;

public class BufferReaderWriterTest {

  @Test
  public void shouldSerializeAndDeserialize() {
    // given
    final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
    final ReaderWriter readerWriter = new ReaderWriter();
    final Serializer<ReaderWriter> serializer = new BufferReaderWriter<>();

    // when
    readerWriter.value = "foo";
    final int length = serializer.serialize(readerWriter, buffer, 0);

    readerWriter.value = "";
    final ReaderWriter deserialized = serializer.deserialize(buffer, 0, length, readerWriter);

    // then
    assertThat(readerWriter.value).isEqualTo("foo");
    assertThat(deserialized).isEqualTo(readerWriter);
  }

  static class ReaderWriter implements BufferWriter, BufferReader {

    String value;

    @Override
    public void wrap(DirectBuffer buffer, int offset, int length) {
      value = buffer.getStringUtf8(offset, length);
    }

    @Override
    public int getLength() {
      return value.getBytes().length;
    }

    @Override
    public void write(MutableDirectBuffer buffer, int offset) {
      buffer.putStringUtf8(offset, value, getLength());
    }
  }
}
