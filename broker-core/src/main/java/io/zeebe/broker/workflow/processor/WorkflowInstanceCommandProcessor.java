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

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.broker.workflow.state.WorkflowEngineState;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;

public class WorkflowInstanceCommandProcessor
    implements TypedRecordProcessor<WorkflowInstanceRecord> {

  private final WorkflowInstanceCommandHandlers commandHandlers;
  private final WorkflowEngineState state;
  private final WorkflowInstanceCommandContext context;

  public WorkflowInstanceCommandProcessor(WorkflowEngineState state) {
    this.state = state;
    this.commandHandlers = new WorkflowInstanceCommandHandlers(state);
    final EventOutput output = new EventOutput(state);
    this.context = new WorkflowInstanceCommandContext(output);
  }

  @Override
  public void processRecord(
      TypedRecord<WorkflowInstanceRecord> record,
      TypedResponseWriter responseWriter,
      TypedStreamWriter streamWriter) {
    populateCommandContext(record, responseWriter, streamWriter);
    commandHandlers.handle(context);
    state.getElementInstanceState().flushDirtyState();
  }

  private void populateCommandContext(
      TypedRecord<WorkflowInstanceRecord> record,
      TypedResponseWriter responseWriter,
      TypedStreamWriter streamWriter) {
    context.setRecord(record);
    context.setResponseWriter(responseWriter);
    context.setStreamWriter(streamWriter);

    final ElementInstance elementInstance =
        state.getElementInstanceState().getInstance(record.getKey());
    context.setElementInstance(elementInstance);
  }
}
