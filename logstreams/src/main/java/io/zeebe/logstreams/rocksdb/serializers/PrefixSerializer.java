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

public interface PrefixSerializer<T> {

  Serializer<T> getPrefixSerializer();

  // Serialization

  default DirectBuffer serializePrefix(T value, MutableDirectBuffer dest, int offset) {
    return getPrefixSerializer().serialize(value, dest, offset);
  }

  default DirectBuffer serializePrefix(T value, MutableDirectBuffer dest) {
    return serializePrefix(value, dest, 0);
  }

  default int getPrefixLength() {
    return getPrefixSerializer().getLength();
  }

  // Deserialization

  default T deserializePrefix(DirectBuffer source, int offset, int length) {
    return getPrefixSerializer().deserialize(source, offset, length);
  }

  default T deserializePrefix(DirectBuffer source, int length) {
    return deserializePrefix(source, 0, length);
  }

  default T deserializePrefix(DirectBuffer source) {
    return deserializePrefix(source, source.capacity());
  }
}
