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

import java.util.ArrayList;
import java.util.List;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;

public class ZbStateDescriptor<S extends ZbState> {
  private final ZbStateSupplier<S> stateSupplier;
  private final List<ZbStateColumnDescriptor> stateColumnDescriptors;

  public ZbStateDescriptor(
      ZbStateSupplier<S> stateSupplier, List<ZbStateColumnDescriptor> stateColumnDescriptors) {
    this.stateSupplier = stateSupplier;
    this.stateColumnDescriptors = stateColumnDescriptors;
  }

  public List<ColumnFamilyDescriptor> getColumnFamilyDescriptors() {
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    for (final ZbStateColumnDescriptor stateColumnDescriptor : stateColumnDescriptors) {
      columnFamilyDescriptors.add(stateColumnDescriptor.getColumnFamilyDescriptor());
    }

    return columnFamilyDescriptors;
  }

  public S get(ZbRocksDb db, List<ColumnFamilyHandle> handles) {
    return stateSupplier.get(db, handles, stateColumnDescriptors);
  }
}
