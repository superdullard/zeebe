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

import io.zeebe.logstreams.rocksdb.serializers.BufferReaderWriter;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.LongProperty;
import io.zeebe.msgpack.property.StringProperty;
import io.zeebe.util.buffer.BufferUtil;
import java.util.Objects;

public class TestUnpackedObject extends UnpackedObject {

  private final LongProperty keyProperty = new LongProperty("key", -1);
  private final StringProperty nameProperty = new StringProperty("name", "");

  public TestUnpackedObject() {
    this.declareProperty(keyProperty).declareProperty(nameProperty);
  }

  public TestUnpackedObject setKey(long key) {
    keyProperty.setValue(key);
    return this;
  }

  public long getKey() {
    return keyProperty.getValue();
  }

  public TestUnpackedObject setName(String name) {
    nameProperty.setValue(name);
    return this;
  }

  public String getName() {
    return BufferUtil.bufferAsString(nameProperty.getValue());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof TestUnpackedObject)) {
      return false;
    }

    final TestUnpackedObject myObject = (TestUnpackedObject) o;
    return Objects.equals(getKey(), myObject.getKey())
        && Objects.equals(getName(), myObject.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKey(), getName());
  }

  public static class Serializer extends BufferReaderWriter<TestUnpackedObject> {

    private final TestUnpackedObject instance = new TestUnpackedObject();
  }
}
