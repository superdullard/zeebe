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

import io.zeebe.logstreams.rocksdb.serializers.buffers.DirectBufferSerializer;
import io.zeebe.logstreams.rocksdb.serializers.buffers.MutableDirectBufferSerializer;
import io.zeebe.logstreams.rocksdb.serializers.primitives.BooleanSerializer;
import io.zeebe.logstreams.rocksdb.serializers.primitives.ByteSerializer;
import io.zeebe.logstreams.rocksdb.serializers.primitives.CharacterSerializer;
import io.zeebe.logstreams.rocksdb.serializers.primitives.IntegerSerializer;
import io.zeebe.logstreams.rocksdb.serializers.primitives.LongSerializer;

public final class Serializers {

  // Primitives
  public static final BooleanSerializer BOOL = new BooleanSerializer();
  public static final ByteSerializer BYTE = new ByteSerializer();
  public static final CharacterSerializer CHAR = new CharacterSerializer();
  public static final IntegerSerializer INT = new IntegerSerializer();
  public static final LongSerializer LONG = new LongSerializer();

  // Buffers
  public static final DirectBufferSerializer BUFFER_WRAP = new DirectBufferSerializer();
  public static final MutableDirectBufferSerializer BUFFER_COPY =
      new MutableDirectBufferSerializer();

  // Collections
  // TODO: implement fixed size collections (i.e. collections whose sizes is known prior to
  // serialization/deserialization
}
