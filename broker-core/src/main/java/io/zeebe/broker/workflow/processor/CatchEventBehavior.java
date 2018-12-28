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
package io.zeebe.broker.workflow.processor;

import static io.zeebe.util.buffer.BufferUtil.bufferAsString;
import static io.zeebe.util.buffer.BufferUtil.cloneBuffer;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.workflow.data.TimerRecord;
import io.zeebe.broker.workflow.model.element.ExecutableCatchEvent;
import io.zeebe.broker.workflow.model.element.ExecutableCatchEventSupplier;
import io.zeebe.broker.workflow.model.element.ExecutableMessage;
import io.zeebe.broker.workflow.state.DeployedWorkflow;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.ElementInstanceState;
import io.zeebe.broker.workflow.state.IndexedRecord;
import io.zeebe.broker.workflow.state.StoredRecord;
import io.zeebe.broker.workflow.state.StoredRecord.Purpose;
import io.zeebe.broker.workflow.state.TimerInstance;
import io.zeebe.broker.workflow.state.WorkflowInstanceSubscription;
import io.zeebe.model.bpmn.util.time.RepeatingInterval;
import io.zeebe.msgpack.query.MsgPackQueryProcessor;
import io.zeebe.msgpack.query.MsgPackQueryProcessor.QueryResult;
import io.zeebe.msgpack.query.MsgPackQueryProcessor.QueryResults;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.TimerIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.util.sched.clock.ActorClock;
import java.util.List;
import org.agrona.DirectBuffer;

public class CatchEventBehavior {

  private final ZeebeState state;
  private final SubscriptionCommandSender subscriptionCommandSender;

  /** Split into multiple files once we have a reasonable amount of event triggers. */
  private final WorkflowInstanceRecord workflowInstanceRecord = new WorkflowInstanceRecord();

  private final TimerRecord timerRecord = new TimerRecord();

  private final MsgPackQueryProcessor queryProcessor = new MsgPackQueryProcessor();
  private final WorkflowInstanceSubscription subscription = new WorkflowInstanceSubscription();

  public CatchEventBehavior(ZeebeState state, SubscriptionCommandSender subscriptionCommandSender) {
    this.state = state;
    this.subscriptionCommandSender = subscriptionCommandSender;
  }

  public void unsubscribeFromEvents(long elementInstanceKey, BpmnStepContext<?> context) {
    // at the moment, the way the state is handled we don't need specific event information to
    // unsubscribe from an event trigger, but once messages are supported it will be necessary.
    unsubscribeFromTimerEvents(elementInstanceKey, context.getOutput().getStreamWriter());
    unsubscribeFromMessageEvents(elementInstanceKey, context);
  }

  public void subscribeToEvents(
      BpmnStepContext<?> context, final ExecutableCatchEventSupplier supplier)
      throws MessageCorrelationKeyException {

    // validate all subscriptions first, in case an incident is raised
    for (ExecutableCatchEvent event : supplier.getEvents()) {
      validateEventSubscription(context, event);
    }

    // if all subscriptions are valid then open the subscriptions
    for (final ExecutableCatchEvent event : supplier.getEvents()) {
      if (event.isTimer()) {
        subscribeToTimerEvent(
            context.getRecord().getKey(),
            context.getRecord().getValue().getWorkflowKey(),
            event.getId(),
            event.getTimer(),
            context.getOutput().getStreamWriter());
      } else if (event.isMessage()) {
        subscribeToMessageEvent(context, event);
      }
    }
  }

  private void validateEventSubscription(BpmnStepContext<?> context, ExecutableCatchEvent event) {
    if (event.isMessage()) {
      extractCorrelationKey(context, event.getMessage());
    }
  }

  public boolean occurEventForElement(
      long elementInstanceKey,
      DirectBuffer eventHandlerId,
      DirectBuffer eventPayload,
      TypedStreamWriter streamWriter) {

    final ElementInstanceState elementInstanceState =
        state.getWorkflowState().getElementInstanceState();
    final StoredRecord tokenEvent = elementInstanceState.getTokenEvent(elementInstanceKey);
    final ElementInstance elementInstance = elementInstanceState.getInstance(elementInstanceKey);

    if (tokenEvent != null && tokenEvent.getPurpose() == Purpose.DEFERRED_TOKEN) {
      // if the event belongs to an intermediate catch event

      final WorkflowInstanceRecord deferredRecord = tokenEvent.getRecord().getValue();
      deferredRecord.setPayload(eventPayload).setElementId(eventHandlerId);

      streamWriter.appendFollowUpEvent(
          elementInstanceKey, WorkflowInstanceIntent.EVENT_OCCURRED, deferredRecord);

      return true;

    } else if (elementInstance != null
        && elementInstance.getState() == WorkflowInstanceIntent.ELEMENT_ACTIVATED) {
      // if the event belongs to a boundary event

      final WorkflowInstanceRecord source = elementInstance.getValue();

      workflowInstanceRecord.wrap(source);
      workflowInstanceRecord.setPayload(eventPayload);
      workflowInstanceRecord.setElementId(eventHandlerId);

      streamWriter.appendFollowUpEvent(
          elementInstanceKey, WorkflowInstanceIntent.EVENT_OCCURRED, workflowInstanceRecord);

      // TODO (saig0): While processing the event a token is consumed. We need to spawn a new one
      // here explicitly - see #1767
      elementInstanceState.getInstance(elementInstance.getParentKey()).spawnToken();
      elementInstanceState.flushDirtyState();

      return true;

    } else {
      // ignore the event if the element is left
      return false;
    }
  }

  public void occurStartEvent(
      long workflowKey,
      DirectBuffer handlerId,
      DirectBuffer eventPayload,
      TypedStreamWriter streamWriter) {
    final DeployedWorkflow workflow = state.getWorkflowState().getWorkflowByKey(workflowKey);

    workflowInstanceRecord.reset();
    workflowInstanceRecord.setVersion(workflow.getVersion());
    workflowInstanceRecord.setWorkflowKey(workflow.getKey());
    workflowInstanceRecord.setBpmnProcessId(workflow.getBpmnProcessId());
    workflowInstanceRecord.setPayload(eventPayload);
    workflowInstanceRecord.setElementId(handlerId);
    streamWriter.appendNewCommand(WorkflowInstanceIntent.CREATE, workflowInstanceRecord);
  }

  public void deferEvent(BpmnStepContext<?> context) {
    if (context.getState() != WorkflowInstanceIntent.EVENT_OCCURRED) {
      throw new IllegalStateException(
          "defer event must be of intent EVENT_OCCURRED but was " + context.getState());
    }

    context.getOutput().deferEvent(context.getRecord());
  }

  public void triggerDeferredEvent(BpmnStepContext<?> context) {
    final TypedRecord<WorkflowInstanceRecord> record = context.getRecord();
    final long elementInstanceKey = record.getKey();
    final long scopeInstanceKey = record.getValue().getScopeInstanceKey();
    final List<IndexedRecord> deferredTokens =
        state.getWorkflowState().getElementInstanceState().getDeferredTokens(scopeInstanceKey);

    for (IndexedRecord token : deferredTokens) {
      if (token.getKey() == elementInstanceKey
          && token.getState() == WorkflowInstanceIntent.EVENT_OCCURRED) {

        context
            .getOutput()
            .appendNewEvent(WorkflowInstanceIntent.EVENT_TRIGGERING, token.getValue());

        context.getOutput().consumeDeferredEvent(scopeInstanceKey, elementInstanceKey);
      }
    }
  }

  public void subscribeToTimerEvent(
      long elementInstanceKey,
      long workflowKey,
      DirectBuffer handlerNodeId,
      RepeatingInterval timer,
      TypedStreamWriter writer) {
    final long nowMs = ActorClock.currentTimeMillis();
    final long dueDate = timer.getInterval().toEpochMilli(nowMs);

    timerRecord
        .setRepetitions(timer.getRepetitions())
        .setDueDate(dueDate)
        .setElementInstanceKey(elementInstanceKey)
        .setHandlerNodeId(handlerNodeId)
        .setWorkflowKey(workflowKey);
    writer.appendNewCommand(TimerIntent.CREATE, timerRecord);
  }

  private void unsubscribeFromTimerEvents(long elementInstanceKey, TypedStreamWriter writer) {
    state
        .getWorkflowState()
        .getTimerState()
        .forEachTimerForElementInstance(
            elementInstanceKey, t -> unsubscribeFromTimerEvent(t, writer));
  }

  public void unsubscribeFromTimerEvent(TimerInstance timer, TypedStreamWriter writer) {
    timerRecord
        .setElementInstanceKey(timer.getElementInstanceKey())
        .setDueDate(timer.getDueDate())
        .setHandlerNodeId(timer.getHandlerNodeId())
        .setWorkflowKey(timer.getWorkflowKey());

    writer.appendFollowUpCommand(timer.getKey(), TimerIntent.CANCEL, timerRecord);
  }

  private void subscribeToMessageEvent(BpmnStepContext<?> context, ExecutableCatchEvent handler) {
    final ExecutableMessage message = handler.getMessage();
    final DirectBuffer extractedKey = extractCorrelationKey(context, message);

    final long workflowInstanceKey = context.getValue().getWorkflowInstanceKey();
    final long elementInstanceKey = context.getRecord().getKey();
    final DirectBuffer messageName = cloneBuffer(message.getMessageName());
    final DirectBuffer correlationKey = cloneBuffer(extractedKey);
    final boolean closeOnCorrelate = true; // todo (npepinpe): will updated in #1592

    subscription.setMessageName(messageName);
    subscription.setElementInstanceKey(elementInstanceKey);
    subscription.setCommandSentTime(ActorClock.currentTimeMillis());
    subscription.setWorkflowInstanceKey(workflowInstanceKey);
    subscription.setCorrelationKey(correlationKey);
    subscription.setHandlerNodeId(handler.getId());
    subscription.setCloseOnCorrelate(closeOnCorrelate);
    state.getWorkflowInstanceSubscriptionState().put(subscription);

    context
        .getSideEffect()
        .add(
            () ->
                sendOpenMessageSubscription(
                    workflowInstanceKey,
                    elementInstanceKey,
                    messageName,
                    correlationKey,
                    closeOnCorrelate));
  }

  private void unsubscribeFromMessageEvents(long elementInstanceKey, BpmnStepContext<?> context) {
    state
        .getWorkflowInstanceSubscriptionState()
        .visitElementSubscriptions(
            elementInstanceKey, sub -> unsubscribeFromMessageEvent(context, sub));
  }

  private boolean unsubscribeFromMessageEvent(
      BpmnStepContext<?> context, WorkflowInstanceSubscription subscription) {
    final DirectBuffer messageName = cloneBuffer(subscription.getMessageName());
    final int subscriptionPartitionId = subscription.getSubscriptionPartitionId();
    final long workflowInstanceKey = subscription.getWorkflowInstanceKey();
    final long elementInstanceKey = subscription.getElementInstanceKey();

    subscription.setClosing();
    state
        .getWorkflowInstanceSubscriptionState()
        .updateToClosingState(subscription, ActorClock.currentTimeMillis());

    context
        .getSideEffect()
        .add(
            () ->
                sendCloseMessageSubscriptionCommand(
                    subscriptionPartitionId, workflowInstanceKey, elementInstanceKey, messageName));

    return true;
  }

  private DirectBuffer extractCorrelationKey(
      BpmnStepContext<?> context, ExecutableMessage message) {
    final QueryResults results =
        queryProcessor.process(message.getCorrelationKey(), context.getValue().getPayload());
    final String errorMessage;

    if (results.size() == 1) {
      final QueryResult result = results.getSingleResult();
      if (result.isString()) {
        return result.getString();
      }

      if (result.isLong()) {
        return result.getLongAsString();
      }

      errorMessage = "the value must be either a string or a number";
    } else if (results.size() > 1) {
      errorMessage = "multiple values found";
    } else {
      errorMessage = "no value found";
    }

    final String expression = bufferAsString(message.getCorrelationKey().getExpression());
    final String failureMessage =
        String.format(
            "Failed to extract the correlation-key by '%s': %s", expression, errorMessage);
    throw new MessageCorrelationKeyException(failureMessage);
  }

  private boolean sendCloseMessageSubscriptionCommand(
      int subscriptionPartitionId,
      long workflowInstanceKey,
      long elementInstanceKey,
      DirectBuffer messageName) {
    return subscriptionCommandSender.closeMessageSubscription(
        subscriptionPartitionId, workflowInstanceKey, elementInstanceKey, messageName);
  }

  private boolean sendOpenMessageSubscription(
      long workflowInstanceKey,
      long elementInstanceKey,
      DirectBuffer messageName,
      DirectBuffer correlationKey,
      boolean closeOnCorrelate) {
    return subscriptionCommandSender.openMessageSubscription(
        workflowInstanceKey, elementInstanceKey, messageName, correlationKey, closeOnCorrelate);
  }

  public class MessageCorrelationKeyException extends RuntimeException {

    public MessageCorrelationKeyException(String message) {
      super(message);
    }
  }
}
