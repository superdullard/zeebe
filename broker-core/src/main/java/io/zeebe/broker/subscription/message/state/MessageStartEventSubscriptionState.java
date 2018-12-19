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

import io.zeebe.broker.logstreams.state.ZbColumnFamilies;
import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.DbCompositeKey;
import io.zeebe.db.impl.DbLong;
import io.zeebe.db.impl.DbNil;
import io.zeebe.db.impl.DbString;
import org.agrona.DirectBuffer;

public class MessageStartEventSubscriptionState {

  private final ZeebeDb<ZbColumnFamilies> zeebeDb;

  private final DbString messageName;
  private final DbLong workflowKey;

  // (messageName, workflowKey => MessageSubscription)
  private final DbCompositeKey<DbString, DbLong> messageNameAndWorkflowKey;
  private final MessageStartEventSubscription subscriptionInfo;
  private final ColumnFamily<DbCompositeKey<DbString, DbLong>, MessageStartEventSubscription>
      subscriptionsColumnFamily;

  // (workflowKey, messageName) => \0  : to find existing subscriptions of a workflow
  private final DbCompositeKey<DbLong, DbString>
      workflowKeyAndMessageName;
  private final ColumnFamily<DbCompositeKey<DbLong, DbString>, DbNil>
    subscriptionsOfWorkflowKeyColumnfamily;

  public MessageStartEventSubscriptionState(ZeebeDb<ZbColumnFamilies> zeebeDb) {
    this.zeebeDb = zeebeDb;

    messageName = new DbString();
    workflowKey = new DbLong();
    messageNameAndWorkflowKey = new DbCompositeKey<>(messageName, workflowKey);
    subscriptionInfo = new MessageStartEventSubscription();
    subscriptionsColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_START_EVENT_SUBSCRIPTION_BY_NAME_AND_KEY,
            messageNameAndWorkflowKey,
            subscriptionInfo);

    workflowKeyAndMessageName =
        new DbCompositeKey<>(workflowKey, messageName);
    subscriptionsOfWorkflowKeyColumnfamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_START_EVENT_SUBSCRIPTION_BY_PROCESS_ID_NAME_AND_KEY,
            workflowKeyAndMessageName,
            DbNil.INSTANCE);
  }

  public void put(final MessageStartEventSubscription subscription) {
    zeebeDb.batch(
        () -> {
          messageName.wrapBuffer(subscription.getMessageName());
          workflowKey.wrapLong(subscription.getWorkflowKey());
          subscriptionsColumnFamily.put(messageNameAndWorkflowKey, subscription);
          subscriptionsOfWorkflowKeyColumnfamily.put(
              workflowKeyAndMessageName, DbNil.INSTANCE);
        });
  }

  public void removeSubscriptionsOfWorkflow(long workflowKey) {
    this.workflowKey.wrapLong(workflowKey);

    subscriptionsOfWorkflowKeyColumnfamily.whileEqualPrefix(
        this.workflowKey,
        (key, value) -> {
          subscriptionsColumnFamily.delete(messageNameAndWorkflowKey);
          subscriptionsOfWorkflowKeyColumnfamily.delete(key);
        });
  }

  public boolean exists(final MessageStartEventSubscription subscription) {
    messageName.wrapBuffer(subscription.getMessageName());
    workflowKey.wrapLong(subscription.getWorkflowKey());

    return subscriptionsColumnFamily.exists(messageNameAndWorkflowKey);
  }

  public void visitSubscriptionsByMessageName(
      DirectBuffer messageName, MessageStartEventSubscriptionVisitor visitor) {

    this.messageName.wrapBuffer(messageName);
    subscriptionsColumnFamily.whileEqualPrefix(
        this.messageName,
        (key, value) -> {
          visitor.visit(value);
        });
  }

  @FunctionalInterface
  public interface MessageStartEventSubscriptionVisitor {
    void visit(MessageStartEventSubscription subscription); // TODO: parameter type
  }
}
