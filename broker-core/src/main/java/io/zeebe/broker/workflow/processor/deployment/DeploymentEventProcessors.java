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
package io.zeebe.broker.workflow.processor.deployment;

import static io.zeebe.protocol.intent.DeploymentIntent.CREATE;

import io.zeebe.broker.logstreams.processor.TypedEventStreamProcessorBuilder;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.workflow.processor.CatchEventBehavior;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.protocol.clientapi.ValueType;

public class DeploymentEventProcessors {

  public static void addDeploymentCreateProcessor(
      TypedEventStreamProcessorBuilder processorBuilder, WorkflowState workflowState) {
    processorBuilder.onCommand(
        ValueType.DEPLOYMENT, CREATE, new DeploymentCreateProcessor(workflowState));
  }

  public static void addTransformingDeploymentProcessor(
      TypedEventStreamProcessorBuilder processorBuilder,
      ZeebeState zeebeState,
      CatchEventBehavior catchEventBehavior) {

    processorBuilder.onCommand(
        ValueType.DEPLOYMENT,
        CREATE,
        new TransformingDeploymentCreateProcessor(zeebeState, catchEventBehavior));
  }
}
