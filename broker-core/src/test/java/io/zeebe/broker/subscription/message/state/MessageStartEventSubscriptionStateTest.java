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
    MessageStartEventSubscription subscription =
        createSubscription("messageName", "startEventID", 1);
    state.put(subscription);
    assertThat(state.exists(subscription)).isTrue();
  }

  @Test
  public void shouldNotExistForDifferentKey() {
    MessageStartEventSubscription subscription =
        createSubscription("messageName", "startEventID", 1);
    state.put(subscription);

    subscription.setWorkflowKey(2);
    assertThat(state.exists(subscription)).isFalse();
  }

  @Test
  public void shouldVisitForMessageNames() {
    MessageStartEventSubscription subscription1 =
        createSubscription("message", "startEvent1", 1);
    state.put(subscription1);

    // more subscription for same workflow
    MessageStartEventSubscription subscription2 =
        createSubscription("message", "startEvent2", 2);
    state.put(subscription2);

    MessageStartEventSubscription subscription3 =
        createSubscription("message", "startEvent3", 3);
    state.put(subscription3);

    MessageStartEventSubscription subscription4 =
        createSubscription("message-other", "startEvent4", 3);
    state.put(subscription4);

    List<DirectBuffer> visitedStartEvents = new ArrayList<>();

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
    MessageStartEventSubscription subscription1 =
        createSubscription("message1", "startEvent1", 1);
    state.put(subscription1);

    // more subscription for same workflow
    MessageStartEventSubscription subscription2 =
        createSubscription("message2", "startEvent2", 1);
    state.put(subscription2);

    MessageStartEventSubscription subscription3 =
        createSubscription("message3", "startEvent3", 1);
    state.put(subscription3);


    assertThat(state.exists(subscription1)).isFalse();
    assertThat(state.exists(subscription2)).isFalse();
    assertThat(state.exists(subscription3)).isFalse();
  }

  @Test
  public void shouldNotRemoveOtherKeys() {
    MessageStartEventSubscription subscription1 =
      createSubscription("message1", "startEvent1", 1);
    state.put(subscription1);

    MessageStartEventSubscription subscription2 =
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
