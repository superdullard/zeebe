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

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.value.MessageStartEventSubscriptionRecordValue;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.builder.ProcessBuilder;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.clientapi.PartitionTestClient;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class MessageStartEventSubscriptionTest {
  private static final String MESSAGE_NAME1 = "startMessage1";
  private static final String EVENT_ID1 = "startEventId1";

  private static final String MESSAGE_NAME2 = "startMessage2";
  private static final String EVENT_ID2 = "startEventId2";

  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();

  public ClientApiRule apiRule = new ClientApiRule(brokerRule::getClientAddress);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

  private PartitionTestClient testClient;

  @Before
  public void init() {
    testClient = apiRule.partitionClient();
  }

  @Test
  public void shouldOpenMessageSubscriptionOnDeployment() {

    // when
    testClient.deploy(createWorkflowWithOneMessageStartEvent());

    final Record<MessageStartEventSubscriptionRecordValue> subscription =
        RecordingExporter.messageStartEventSubscriptionRecords(MessageSubscriptionIntent.OPENED)
            .getFirst();

    // then
    assertThat(subscription.getValue().getStartEventId()).isEqualTo(EVENT_ID1);
    assertThat(subscription.getValue().getMessageName()).isEqualTo(MESSAGE_NAME1);
  }

  @Test
  public void shouldOpenSubscriptionsForAllMessageStartEvents() {

    // when
    testClient.deploy(createWorkflowWithTwoMessageStartEvent());

    final List<Record<MessageStartEventSubscriptionRecordValue>> subscriptions =
        RecordingExporter.messageStartEventSubscriptionRecords(MessageSubscriptionIntent.OPENED)
            .limit(2)
            .asList();

    // then
    assertThat(subscriptions.size()).isEqualTo(2);

    final MessageStartEventSubscriptionRecordValue firstSubscription =
        subscriptions.get(0).getValue();
    final MessageStartEventSubscriptionRecordValue secondSubscription =
        subscriptions.get(1).getValue();

    assertThat(firstSubscription.getMessageName()).isEqualTo(MESSAGE_NAME2);
    assertThat(firstSubscription.getStartEventId()).isEqualTo(EVENT_ID2);

    assertThat(secondSubscription.getMessageName()).isEqualTo(MESSAGE_NAME1);
    assertThat(secondSubscription.getStartEventId()).isEqualTo(EVENT_ID1);

    assertThat(firstSubscription.getWorkflowKey()).isEqualTo(secondSubscription.getWorkflowKey());
  }

  private static BpmnModelInstance createWorkflowWithOneMessageStartEvent() {
    final BpmnModelInstance modelInstance =
        Bpmn.createExecutableProcess("processId")
            .startEvent(EVENT_ID1)
            .message(m -> m.name(MESSAGE_NAME1).id("startmsgId"))
            .endEvent()
            .done();

    return modelInstance;
  }

  private static BpmnModelInstance createWorkflowWithTwoMessageStartEvent() {
    final ProcessBuilder process = Bpmn.createExecutableProcess("processId");
    process.startEvent(EVENT_ID1).message(m -> m.name(MESSAGE_NAME1).id("startmsgId1")).endEvent();
    process.startEvent(EVENT_ID2).message(m -> m.name(MESSAGE_NAME2).id("startmsgId2")).endEvent();

    final BpmnModelInstance modelInstance = process.done();
    return modelInstance;
  }
}
