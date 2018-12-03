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
package io.zeebe.logstreams.rocksdb;

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.zeebe.logstreams.rocksdb.serializers.Serializer;
import io.zeebe.logstreams.rocksdb.serializers.Serializers;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

public class ZbColumnEntryTest {

  private final Serializer<DirectBuffer> keySerializer = spy(Serializers.BUFFER_WRAP);
  private final Serializer<DirectBuffer> valueSerializer = spy(Serializers.BUFFER_WRAP);

  @Test
  public void shouldNotInitiallyBeSet() {
    // given
    final ZbColumnEntry<DirectBuffer, DirectBuffer> entry =
        new ZbColumnEntry<>(keySerializer, valueSerializer);

    // then
    assertThat(entry.isSet()).isFalse();
  }

  @Test
  public void shouldSetBuffersWithoutDeserializing() {
    // given
    final ZbColumnEntry<DirectBuffer, DirectBuffer> entry =
        new ZbColumnEntry<>(keySerializer, valueSerializer);
    final DirectBuffer key = wrapString("foo");
    final DirectBuffer value = wrapString("bar");

    // when
    entry.set(key, value);

    // then
    assertThat(entry.isSet()).isTrue();
    verify(keySerializer, never()).deserialize(any(DirectBuffer.class), anyInt(), anyInt(), any());
    verify(valueSerializer, never())
        .deserialize(any(DirectBuffer.class), anyInt(), anyInt(), any());
  }

  @Test
  public void shouldDeserializeKey() {
    // given
    final DirectBuffer view = new UnsafeBuffer();
    final ZbColumnEntry<DirectBuffer, DirectBuffer> entry =
        new ZbColumnEntry<>(keySerializer, view, valueSerializer);
    final DirectBuffer key = serialize("foo");
    final DirectBuffer value = serialize("bar");

    // when
    entry.set(key, value);
    final DirectBuffer deserialized = entry.getKey();

    // then
    assertThat(deserialized).isEqualTo(wrapString("foo"));
  }

  @Test
  public void shouldDeserializeKeyOnFirstGet() {
    // given
    final DirectBuffer view = new UnsafeBuffer();
    final ZbColumnEntry<DirectBuffer, DirectBuffer> entry =
        new ZbColumnEntry<>(keySerializer, view, valueSerializer);
    final DirectBuffer key = serialize("foo");
    final DirectBuffer value = serialize("bar");

    // when
    entry.set(key, value);
    final DirectBuffer first = entry.getKey();
    final DirectBuffer second = entry.getKey();

    // then
    assertThat(first).isSameAs(second);
    verify(keySerializer, times(1)).deserialize(eq(key), eq(0), eq(value.capacity()), eq(view));
  }

  @Test
  public void shouldDeserializeValue() {
    // given
    final DirectBuffer view = new UnsafeBuffer();
    final ZbColumnEntry<DirectBuffer, DirectBuffer> entry =
        new ZbColumnEntry<>(keySerializer, valueSerializer, view);
    final DirectBuffer key = serialize("foo");
    final DirectBuffer value = serialize("bar");

    // when
    entry.set(key, value);
    final DirectBuffer deserialized = entry.getValue();

    // then
    assertThat(deserialized).isEqualTo(wrapString("bar"));
  }

  @Test
  public void shouldDeserializeValueOnFirstGet() {
    // given
    final DirectBuffer view = new UnsafeBuffer();
    final ZbColumnEntry<DirectBuffer, DirectBuffer> entry =
        new ZbColumnEntry<>(keySerializer, valueSerializer, view);
    final DirectBuffer key = serialize("foo");
    final DirectBuffer value = serialize("bar");

    // when
    entry.set(key, value);
    final DirectBuffer first = entry.getValue();
    final DirectBuffer second = entry.getValue();

    // then
    assertThat(first).isSameAs(second);
    verify(valueSerializer, times(1)).deserialize(eq(value), eq(0), eq(value.capacity()), eq(view));
  }

  private DirectBuffer serialize(String value) {
    final DirectBuffer view = new UnsafeBuffer();
    final MutableDirectBuffer buffer = new ExpandableArrayBuffer();

    Serializers.BUFFER_WRAP.serialize(wrapString(value), buffer, 0, view);
    return view;
  }
}
