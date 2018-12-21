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

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.util.ZeebeStateRule;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class MessageStartEventSubscriptionStateTest {

  @Rule public ZeebeStateRule stateRule = new ZeebeStateRule();

  private MessageStartEventSubscriptionState state;

  @Before
  public void setUp() {
    state = stateRule.getZeebeState().getMessageStartEventSubscriptionState();
  }

  @Test
  public void shouldExistAfterPut() {
    final MessageStartEventSubscription subscription =
        createSubscription("messageName", "startEventID", 1);
    state.put(subscription);
    assertThat(state.exists(subscription)).isTrue();
  }

  @Test
  public void shouldNotExistForDifferentKey() {
    final MessageStartEventSubscription subscription =
        createSubscription("messageName", "startEventID", 1);
    state.put(subscription);

    subscription.setWorkflowKey(2);
    assertThat(state.exists(subscription)).isFalse();
  }

  @Test
  public void shouldVisitForMessageNames() {
    final MessageStartEventSubscription subscription1 =
        createSubscription("message", "startEvent1", 1);
    state.put(subscription1);

    // more subscription for same workflow
    final MessageStartEventSubscription subscription2 =
        createSubscription("message", "startEvent2", 2);
    state.put(subscription2);

    final MessageStartEventSubscription subscription3 =
        createSubscription("message", "startEvent3", 3);
    state.put(subscription3);

    final MessageStartEventSubscription subscription4 =
        createSubscription("message-other", "startEvent4", 3);
    state.put(subscription4);

    final List<DirectBuffer> visitedStartEvents = new ArrayList<>();

    state.visitSubscriptionsByMessageName(
        wrapString("message"),
        subscription -> {
          visitedStartEvents.add(subscription.getStartEventId());
        });

    assertThat(visitedStartEvents.size()).isEqualTo(3);
    assertThat(visitedStartEvents.contains(wrapString("startEvent1")));
    assertThat(visitedStartEvents.contains(wrapString("startEvent2")));
    assertThat(visitedStartEvents.contains(wrapString("startEvent3")));
  }

  @Test
  public void shouldNotExistAfterRemove() {
    final MessageStartEventSubscription subscription1 =
        createSubscription("message1", "startEvent1", 1);
    state.put(subscription1);

    // more subscription for same workflow
    final MessageStartEventSubscription subscription2 =
        createSubscription("message2", "startEvent2", 1);
    state.put(subscription2);

    final MessageStartEventSubscription subscription3 =
        createSubscription("message3", "startEvent3", 1);
    state.put(subscription3);

    assertThat(state.exists(subscription1)).isFalse();
    assertThat(state.exists(subscription2)).isFalse();
    assertThat(state.exists(subscription3)).isFalse();
  }

  @Test
  public void shouldNotRemoveOtherKeys() {
    final MessageStartEventSubscription subscription1 =
        createSubscription("message1", "startEvent1", 1);
    state.put(subscription1);

    final MessageStartEventSubscription subscription2 =
        createSubscription("message1", "startEvent1", 4);
    state.put(subscription2);

    state.removeSubscriptionsOfWorkflow(1);

    assertThat(state.exists(subscription1)).isFalse();
    assertThat(state.exists(subscription2)).isTrue();
  }

  private MessageStartEventSubscription createSubscription(
      String messageName, String startEventId, long key) {
    return new MessageStartEventSubscription(
        wrapString(messageName), wrapString(startEventId), key);
  }
}
