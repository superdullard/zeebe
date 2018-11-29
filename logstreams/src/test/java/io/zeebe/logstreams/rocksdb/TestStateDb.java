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

import java.util.Collections;
import java.util.List;

public class TestStateDb extends ZbStateDb {
  private static final byte[] TEST_STATE_PREFIX = "testState".getBytes();
  private static final List<ZbStateDescriptor> STATE_DESCRIPTORS =
      Collections.singletonList(
          new ZbStateDescriptor<>(TestState::new, TestState.getDescriptors(TEST_STATE_PREFIX)));

  private final TestState testState;

  public TestStateDb(String path, boolean reopen) {
    super(path, reopen);

    testState = (TestState) states.get(0);
  }

  @Override
  protected List<ZbStateDescriptor> getStateDescriptors() {
    return STATE_DESCRIPTORS;
  }

  public TestState getTestState() {
    return testState;
  }
}
