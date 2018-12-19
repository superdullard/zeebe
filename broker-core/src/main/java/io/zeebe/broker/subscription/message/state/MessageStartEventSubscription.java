/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.subscription.message.state;

import static io.zeebe.db.impl.ZeebeDbConstants.ZB_DB_BYTE_ORDER;
import static io.zeebe.util.buffer.BufferUtil.readIntoBuffer;
import static io.zeebe.util.buffer.BufferUtil.writeIntoBuffer;

import io.zeebe.db.DbValue;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class MessageStartEventSubscription implements DbValue {

  private long workflowKey;
  private final DirectBuffer messageName = new UnsafeBuffer();
  private final DirectBuffer startEventId = new UnsafeBuffer();

  public MessageStartEventSubscription() {}

  public MessageStartEventSubscription(
      DirectBuffer messageName,
      DirectBuffer startEventId,
      long workflowKey) {
    this.messageName.wrap(messageName);
    this.workflowKey = workflowKey;
    this.startEventId.wrap(startEventId);
  }

  @Override
  public int getLength() {
    return Long.BYTES + Integer.BYTES * 2 + messageName.capacity() + startEventId.capacity();
  }

  @Override
  public void write(MutableDirectBuffer buffer, int offset) {
    buffer.putLong(offset, workflowKey, ZB_DB_BYTE_ORDER);
    offset += Long.BYTES;

    offset = writeIntoBuffer(buffer, offset, messageName);
    offset = writeIntoBuffer(buffer, offset, startEventId);
    assert offset == getLength() : "End offset differs with getLength()";
  }

  @Override
  public void wrap(DirectBuffer buffer, int offset, int length) {
    this.workflowKey = buffer.getLong(offset, ZB_DB_BYTE_ORDER);
    offset += Long.BYTES;

    offset = readIntoBuffer(buffer, offset, messageName);
    offset = readIntoBuffer(buffer, offset, startEventId);
  }

  public long getWorkflowKey() {
    return workflowKey;
  }

  public MessageStartEventSubscription setWorkflowKey(long workflowKey) {
    this.workflowKey = workflowKey;
    return this;
  }

  public DirectBuffer getMessageName() {
    return messageName;
  }

  public MessageStartEventSubscription setMessageName(DirectBuffer messageName) {
    this.messageName.wrap(messageName);
    return this;
  }

  public DirectBuffer getStartEventId() {
    return startEventId;
  }

  public MessageStartEventSubscription setStartEventId(DirectBuffer startEventId) {
    this.startEventId.wrap(startEventId);
    return this;
  }
}
