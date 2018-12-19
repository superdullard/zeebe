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
package io.zeebe.broker.subscription.message;

import static io.zeebe.test.util.TestUtil.waitUntil;
import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.subscription.message.data.MessageStartEventSubscriptionRecord;
import io.zeebe.broker.subscription.message.processor.MessageEventProcessors;
import io.zeebe.broker.util.StreamProcessorControl;
import io.zeebe.broker.util.StreamProcessorRule;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MessageStartEventSubscriptionProcessorTest {
  @Rule
  public StreamProcessorRule rule = new StreamProcessorRule(Protocol.DEPLOYMENT_PARTITION + 1);

  @Mock private SubscriptionCommandSender mockSubscriptionCommandSender;
  @Mock private TopologyManager mockTopologyManager;
  private StreamProcessorControl streamProcessor;

  @Before
  public void setup() {

    MockitoAnnotations.initMocks(this);

    streamProcessor =
        rule.runStreamProcessor(
            (typedEventStreamProcessorBuilder, zeebeDb) -> {
              MessageEventProcessors.addMessageProcessors(
                  typedEventStreamProcessorBuilder,
                  new ZeebeState(zeebeDb),
                  mockSubscriptionCommandSender,
                  mockTopologyManager);
              return typedEventStreamProcessorBuilder.build();
            });
  }

  @Test
  public void shouldOpenMessageStartEventSubscription() {
    final MessageStartEventSubscriptionRecord subscriptionRecord =
        new MessageStartEventSubscriptionRecord();
    subscriptionRecord
        .setMessageName(wrapString("startMessage"))
        .setStartEventId(wrapString("startEventId"))
        .setWorkflowKey(4);

    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscriptionRecord);

    waitUntil(
        () ->
            rule.events()
                .onlyMessageStartEventSubscriptionRecords()
                .withIntent(MessageSubscriptionIntent.OPENED)
                .findFirst()
                .isPresent());

    final TypedRecord<MessageStartEventSubscriptionRecord> s =
        rule.events()
            .onlyMessageStartEventSubscriptionRecords()
            .withIntent(MessageSubscriptionIntent.OPENED)
            .findFirst()
            .get();

    assertThat(s.getValue().getMessageName()).isEqualTo(wrapString("startMessage"));
    assertThat(s.getValue().getStartEventId()).isEqualTo(wrapString("startEventId"));
  }

  @Test
  public void shouldOpenSubscriptionsForMultipleWorkflowsWithSameMessageName() {
    writeSubscriptionOpenCommand("startMessage", "startEventId",  4);
    writeSubscriptionOpenCommand("startMessage", "startEventId",  8);

    waitUntil(
        () ->
            rule.events()
                    .onlyMessageStartEventSubscriptionRecords()
                    .withIntent(MessageSubscriptionIntent.OPENED)
                    .count()
                == 2);

    List<TypedRecord<MessageStartEventSubscriptionRecord>> subscriptions = rule.events()
      .onlyMessageStartEventSubscriptionRecords()
      .withIntent(MessageSubscriptionIntent.OPENED).asList();

    TypedRecord<MessageStartEventSubscriptionRecord> firstSubscription = subscriptions.get(0);
    TypedRecord<MessageStartEventSubscriptionRecord> secondSubscription = subscriptions.get(1);

    assertThat(firstSubscription.getValue().getMessageName()).isEqualTo(wrapString("startMessage"));
    assertThat(firstSubscription.getValue().getStartEventId()).isEqualTo(wrapString("startEventId"));
  assertThat(secondSubscription.getValue().getMessageName()).isEqualTo(wrapString("startMessage"));
    assertThat(secondSubscription.getValue().getStartEventId()).isEqualTo(wrapString("startEventId"));
  }

  @Test
  public void shouldCloseSubscriptionsForOlderVersions() {
    fail("not implemented");
  }

  private void writeSubscriptionOpenCommand(
      String messageName, String startEventId, long workflowKey) {
    final MessageStartEventSubscriptionRecord subscriptionRecord =
        new MessageStartEventSubscriptionRecord();
    subscriptionRecord
        .setMessageName(wrapString(messageName))
        .setStartEventId(wrapString(startEventId))
        .setWorkflowKey(workflowKey);

    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscriptionRecord);
  }
}
