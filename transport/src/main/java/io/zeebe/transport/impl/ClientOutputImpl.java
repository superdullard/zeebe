/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.transport.impl;

import io.zeebe.transport.ClientOutput;
import io.zeebe.transport.ClientResponse;
import io.zeebe.transport.EndpointRegistry;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.impl.sender.OutgoingMessage;
import io.zeebe.transport.impl.sender.OutgoingRequest;
import io.zeebe.transport.impl.sender.Sender;
import io.zeebe.transport.impl.sender.TransportHeaderWriter;
import io.zeebe.util.buffer.BufferWriter;
import io.zeebe.util.sched.clock.ActorClock;
import io.zeebe.util.sched.future.ActorFuture;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class ClientOutputImpl implements ClientOutput {
  protected final EndpointRegistry endpointRegistry;
  protected final Sender requestManager;
  protected final Duration defaultRequestRetryTimeout;
  protected final long defaultMessageRetryTimeoutInMillis;

  public ClientOutputImpl(
      EndpointRegistry endpointRegistry,
      Sender requestManager,
      Duration defaultRequestRetryTimeout,
      Duration defaultMessageRetryTimeout) {
    this.endpointRegistry = endpointRegistry;
    this.requestManager = requestManager;
    this.defaultRequestRetryTimeout = defaultRequestRetryTimeout;
    this.defaultMessageRetryTimeoutInMillis = defaultMessageRetryTimeout.toMillis();
  }

  @Override
  public boolean sendMessage(Integer nodeId, BufferWriter writer) {
    final RemoteAddress remoteAddress = endpointRegistry.getEndpoint(nodeId);
    if (remoteAddress != null) {
      return sendTransportMessage(remoteAddress.getStreamId(), writer);
    } else {
      return false;
    }
  }

  private boolean sendTransportMessage(int remoteStreamId, BufferWriter writer) {
    final int framedMessageLength =
        TransportHeaderWriter.getFramedMessageLength(writer.getLength());
    final ByteBuffer allocatedBuffer = requestManager.allocateMessageBuffer(framedMessageLength);

    if (allocatedBuffer != null) {
      try {
        final UnsafeBuffer bufferView = new UnsafeBuffer(allocatedBuffer);
        final TransportHeaderWriter headerWriter = new TransportHeaderWriter();
        headerWriter.wrapMessage(bufferView, writer, remoteStreamId);
        final long deadline = ActorClock.currentTimeMillis() + defaultMessageRetryTimeoutInMillis;

        final OutgoingMessage outgoingMessage =
            new OutgoingMessage(remoteStreamId, bufferView, deadline);

        requestManager.submitMessage(outgoingMessage);

        return true;
      } catch (RuntimeException e) {
        requestManager.reclaimMessageBuffer(allocatedBuffer);
        throw e;
      }
    } else {
      return false;
    }
  }

  @Override
  public ActorFuture<ClientResponse> sendRequest(Integer nodeId, BufferWriter writer) {
    return sendRequest(nodeId, writer, defaultRequestRetryTimeout);
  }

  @Override
  public ActorFuture<ClientResponse> sendRequest(
      Integer nodeId, BufferWriter writer, Duration timeout) {
    return sendRequestWithRetry(() -> nodeId, (b) -> false, writer, timeout);
  }

  @Override
  public ActorFuture<ClientResponse> sendRequestWithRetry(
      Supplier<Integer> nodeIdSupplier,
      Predicate<DirectBuffer> responseInspector,
      BufferWriter writer,
      Duration timeout) {
    final int messageLength = writer.getLength();
    final int framedLength = TransportHeaderWriter.getFramedRequestLength(messageLength);

    final ByteBuffer allocatedBuffer = requestManager.allocateRequestBuffer(framedLength);

    if (allocatedBuffer != null) {
      try {
        final UnsafeBuffer bufferView = new UnsafeBuffer(allocatedBuffer);
        final OutgoingRequest request =
            new OutgoingRequest(
                () -> endpointRegistry.getEndpoint(nodeIdSupplier.get()),
                responseInspector,
                bufferView,
                timeout);

        request.getHeaderWriter().wrapRequest(bufferView, writer);

        return requestManager.submitRequest(request);
      } catch (RuntimeException e) {
        requestManager.reclaimRequestBuffer(allocatedBuffer);
        throw e;
      }
    } else {
      return null;
    }
  }
}
