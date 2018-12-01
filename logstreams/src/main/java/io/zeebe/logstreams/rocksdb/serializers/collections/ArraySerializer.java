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
package io.zeebe.logstreams.rocksdb.serializers.collections;

import io.zeebe.logstreams.rocksdb.serializers.CollectionSerializer;
import io.zeebe.logstreams.rocksdb.serializers.Serializer;

public class ArraySerializer<T> extends CollectionSerializer<T[], T> {

  public ArraySerializer(Serializer<T> itemSerializer) {
    super(itemSerializer, ArraySerializer::get);
  }

  public ArraySerializer(Serializer<T> itemSerializer, ItemInstanceSupplier<T[], T> itemSupplier) {
    super(itemSerializer, itemSupplier);
  }

  @Override
  protected int getSize(T[] array) {
    return array.length;
  }

  @Override
  protected void setItem(T[] array, int index, T item) {
    array[index] = item;
  }

  @Override
  protected T getItem(T[] array, int index) {
    return get(array, index);
  }

  private static <T> T get(T[] array, int index) {
    return array[index];
  }
}
