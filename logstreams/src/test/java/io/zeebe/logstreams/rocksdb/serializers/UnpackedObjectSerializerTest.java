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

import io.zeebe.logstreams.rocksdb.TestUnpackedObject;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Test;

public class UnpackedObjectSerializerTest {

  @Test
  public void shouldSerializeAndDeserialize() {
    // given
    final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
    final TestUnpackedObject original =
        new TestUnpackedObject().setKey(3).setName("Max Mustermann");
    final Serializer<TestUnpackedObject> serializer = new UnpackedObjectSerializer<>();

    // when
    final int length = serializer.serialize(original, buffer, 0);
    final TestUnpackedObject deserialized =
        serializer.deserialize(buffer, 0, length, new TestUnpackedObject());

    // then
    assertThat(deserialized).isEqualTo(original);
  }
}
