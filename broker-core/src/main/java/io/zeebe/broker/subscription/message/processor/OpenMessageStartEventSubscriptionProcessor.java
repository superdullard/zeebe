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
package io.zeebe.broker.subscription.message.processor;

import io.zeebe.broker.logstreams.processor.SideEffectProducer;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.subscription.message.data.MessageStartEventSubscriptionRecord;
import io.zeebe.broker.subscription.message.state.MessageStartEventSubscription;
import io.zeebe.broker.subscription.message.state.MessageStartEventSubscriptionState;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;

public class OpenMessageStartEventSubscriptionProcessor
    implements TypedRecordProcessor<MessageStartEventSubscriptionRecord> {

  private final MessageStartEventSubscriptionState subscriptionState;

  public OpenMessageStartEventSubscriptionProcessor(
      MessageStartEventSubscriptionState subscriptionState) {
    this.subscriptionState = subscriptionState;
  }

  @Override
  public void processRecord(
      TypedRecord<MessageStartEventSubscriptionRecord> record,
      TypedResponseWriter responseWriter,
      TypedStreamWriter streamWriter,
      Consumer<SideEffectProducer> sideEffect) {
    // TODO Auto-generated method stub

    final MessageStartEventSubscriptionRecord subscriptionRecord = record.getValue();
    final DirectBuffer messageName = subscriptionRecord.getMessageName();
    final long workflowKey = subscriptionRecord.getWorkflowKey();
    final MessageStartEventSubscription subscription =
        new MessageStartEventSubscription(
            messageName,
            subscriptionRecord.getStartEventId(),
            workflowKey);

    // Check if there exists a subscription for the same message and same workflowKey
    if (!subscriptionState.exists(subscription)) {
      subscriptionState.put(subscription);

      // Write Event MessageStartEventSubscription.OPENED
      streamWriter.appendFollowUpEvent(
          record.getKey(), MessageSubscriptionIntent.OPENED, subscriptionRecord);

    } else {
      // Write command rejected, subscription already exists
      streamWriter.appendRejection(
          record, RejectionType.NOT_APPLICABLE, "Subscription is already open");
    }
  }
}
