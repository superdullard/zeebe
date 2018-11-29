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
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDBException;

@SuppressWarnings("unchecked")
public abstract class ZbStateDb implements AutoCloseable {

  /** to extend, see {@link DBOptions#DBOptions(DBOptions)} */
  public static final DBOptions DEFAULT_OPTIONS = new DBOptions().setFailIfOptionsFileError(true);

  private final List<ZbStateDescriptor> stateDescriptors;
  protected final List<ZbState> states;
  private final DBOptions options;
  private final ZbRocksDb db;

  public ZbStateDb(String path, boolean reopen) {
    this(DEFAULT_OPTIONS, path, reopen);
  }

  public ZbStateDb(DBOptions options, String path, boolean reopen) {
    this.options = new DBOptions(options).setCreateIfMissing(!reopen);

    if (!reopen) {
      this.options.setErrorIfExists(true);
    }

    this.stateDescriptors = getStateDescriptors();
    assert this.stateDescriptors != null && !this.stateDescriptors.isEmpty()
        : "no states described for state database";

    this.states = new ArrayList<>(this.stateDescriptors.size());

    this.db = open(path);
  }

  @Override
  public void close() {
    states.forEach(ZbState::close);
    options.close();
    db.close();
  }

  protected ZbRocksDb open(String path) {
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    final List<IndexRange> indexRanges = new ArrayList<>();
    final ZbRocksDb db;

    buildColumnFamilyDescriptorList(columnFamilyDescriptors, indexRanges);
    db = openDatabase(path, columnFamilyDescriptors, columnFamilyHandles);
    createStates(columnFamilyHandles, indexRanges);

    return db;
  }

  protected abstract List<ZbStateDescriptor> getStateDescriptors();

  private ZbRocksDb openDatabase(
      String path,
      List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      List<ColumnFamilyHandle> columnFamilyHandles) {
    final ZbRocksDb db;

    try {
      db = ZbRocksDb.open(options, path, columnFamilyDescriptors, columnFamilyHandles);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }

    return db;
  }

  private void createStates(
      List<ColumnFamilyHandle> columnFamilyHandles, List<IndexRange> indexRanges) {
    for (int i = 0; i < stateDescriptors.size(); i++) {
      final IndexRange range = indexRanges.get(i);
      final List<ColumnFamilyHandle> handles = columnFamilyHandles.subList(range.begin, range.end);
      final ZbStateDescriptor descriptor = stateDescriptors.get(i);

      states.add(descriptor.get(db, handles));
    }
  }

  private void buildColumnFamilyDescriptorList(
      List<ColumnFamilyDescriptor> combinedColumnFamilyDescriptors, List<IndexRange> indexRanges) {
    int lastIndex = 0;

    for (final ZbStateDescriptor<?> descriptor : stateDescriptors) {
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          descriptor.getColumnFamilyDescriptors();
      final int descriptorsCount = columnFamilyDescriptors.size();
      indexRanges.add(new IndexRange(lastIndex, lastIndex + descriptorsCount));
      combinedColumnFamilyDescriptors.addAll(columnFamilyDescriptors);
      lastIndex += descriptorsCount;
    }
  }

  static class IndexRange {
    final int begin;
    final int end;

    IndexRange(int begin, int end) {
      this.begin = begin;
      this.end = end;
    }
  }
}
