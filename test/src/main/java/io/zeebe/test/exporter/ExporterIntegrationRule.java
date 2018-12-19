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
package io.zeebe.test.exporter;

import static io.zeebe.test.util.record.RecordingExporter.workflowInstanceRecords;

import com.moandjiezana.toml.Toml;
import com.moandjiezana.toml.TomlWriter;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.ExporterCfg;
import io.zeebe.client.ClientProperties;
import io.zeebe.client.api.subscription.JobHandler;
import io.zeebe.client.api.subscription.JobWorker;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.value.IncidentRecordValue;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.ClientRule;
import io.zeebe.test.EmbeddedBrokerRule;
import io.zeebe.test.util.TestUtil;
import io.zeebe.test.util.record.RecordingExporter;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.rules.ExternalResource;

/**
 * JUnit test rule to facilitate running integration tests for exporters.
 *
 * <p>Sets up an embedded broker, gateway, client, and provides convenience methods to:
 *
 * <ol>
 *   <li>run a sample workload (e.g. deploy & create workflows, setup a job worker, etc.)
 *   <li>visit all exported records so far
 *   <li>perform simple operations such as deployments, starting a job worker, etc.
 * </ol>
 *
 * An example integration test suite could look like:
 *
 * <pre>
 * public class MyExporterIT {
 *   @Rule public final ExporterIntegrationRule exporterIntegrationRule = new ExporterIntegrationRule();
 *
 *   @Test
 *   public void shouldExportRecords() {
 *     // given
 *     final MyExporterConfig config = new MyExporterConfig();
 *     exporterIntegrationRule.configure("myExporter", MyExporter.class, config);
 *     exporterIntegrationRule.startBroker();
 *
 *     // when
 *     exporterIntegrationRule.performSampleWorkload();
 *
 *     // then
 *     exporterIntegrationRule.visitExportedRecords(r -> {
 *       // code to assert the record was exported to the external system
 *     });
 *   }
 * }
 * </pre>
 *
 * NOTE: calls to the various configure methods are additive, so it is possible to configure more
 * than one exporter, as long as the IDs are different.
 */
public class ExporterIntegrationRule extends ExternalResource {

  public static final BpmnModelInstance SAMPLE_WORKFLOW =
      Bpmn.createExecutableProcess("testProcess")
          .startEvent()
          .intermediateCatchEvent(
              "message", e -> e.message(m -> m.name("catch").zeebeCorrelationKey("$.orderId")))
          .serviceTask("task", t -> t.zeebeTaskType("work").zeebeTaskHeader("foo", "bar"))
          .endEvent()
          .done();

  private final EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule(false);
  private ClientRule clientRule;
  private boolean configured;

  public BrokerCfg getBrokerCfg() {
    return brokerRule.getBrokerCfg();
  }

  @Override
  protected void after() {
    super.after();
    brokerRule.stopBroker();

    if (clientRule != null) {
      clientRule.destroyClient();
    }
  }

  /**
   * Configures the broker to add whatever exporters are defined in the TOML represented by the
   * input stream.
   *
   * @param toml input stream wrapping a TOML document
   */
  public void configure(InputStream toml) {
    final BrokerCfg config = new Toml().read(toml).to(BrokerCfg.class);
    configure(config.getExporters());
  }

  /**
   * Configures the broker to use the given exporter.
   *
   * @param id the exporter ID
   * @param exporterClass the exporter class
   * @param configuration the configuration to use
   * @param <T> type of the configuration
   * @param <E> type of the exporter
   */
  public <T, E extends Exporter> void configure(
      String id, Class<E> exporterClass, T configuration) {
    final Map<String, Object> arguments =
        new Toml().read(new TomlWriter().write(configuration)).toMap();
    configure(id, exporterClass, arguments);
  }

  /**
   * Configures the broker to use the given exporter.
   *
   * @param id the exporter ID
   * @param exporterClass the exporter class
   * @param arguments the arguments to pass during configuration
   * @param <E> type of the exporter
   */
  public <E extends Exporter> void configure(
      String id, Class<E> exporterClass, Map<String, Object> arguments) {
    final ExporterCfg config = new ExporterCfg();
    config.setId(id);
    config.setClassName(exporterClass.getCanonicalName());
    config.setArgs(arguments);

    configure(Collections.singletonList(config));
  }

  /**
   * Starts the broker. This is blocking and will return once the broker is ready to accept
   * commands.
   *
   * @throws IllegalStateException if no exporter has previously been configured
   */
  public void startBroker() {
    if (!configured) {
      throw new IllegalStateException("No exporter configured!");
    }

    brokerRule.startBroker();
    clientRule = new ClientRule(this::newClientProperties);
    clientRule.createClient();
  }

  /** Runs a sample workload on the broker, exporting several records of different types. */
  public void performSampleWorkload() {
    deployWorkflow(SAMPLE_WORKFLOW, "sample_workflow.bpmn");
    final long workflowInstanceKey =
        createWorkflowInstance("testProcess", Collections.singletonMap("orderId", "foo-bar-123"));

    // create job worker which fails on first try and sets retries to 0 to create an incident
    final AtomicBoolean fail = new AtomicBoolean(true);
    final JobWorker worker =
        createJobWorker(
            "work",
            (client, job) -> {
              if (fail.getAndSet(false)) {
                // fail job
                client.newFailCommand(job.getKey()).retries(0).errorMessage("failed").send().join();
              } else {
                client.newCompleteCommand(job.getKey()).send().join();
              }
            });

    publishMessage("catch", "foo-bar-123");

    // wait for incident and resolve it
    final Record<IncidentRecordValue> incident =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED).getFirst();
    clientRule
        .getClient()
        .newUpdateRetriesCommand(incident.getValue().getJobKey())
        .retries(3)
        .send()
        .join();
    clientRule.getClient().newResolveIncidentCommand(incident.getKey()).send().join();

    // wrap up
    awaitWorkflowCompletion(workflowInstanceKey);
    worker.close();
  }

  /**
   * Visits all exported records in the order they were exported.
   *
   * @param visitor record consumer
   */
  public void visitExportedRecords(Consumer<Record<?>> visitor) {
    RecordingExporter.getRecords().forEach(visitor);
  }

  /**
   * Deploys the given workflow to the broker. Note that the filename must have the "bpmn" file
   * extension, e.g. "resource.bpmn".
   *
   * @param workflow workflow to deploy
   * @param filename resource name, e.g. "workflow.bpmn"
   */
  public void deployWorkflow(BpmnModelInstance workflow, String filename) {
    clientRule.getClient().newDeployCommand().addWorkflowModel(workflow, filename).send().join();
  }

  /**
   * Creates a workflow instance for the given process ID, with the given payload.
   *
   * @param processId BPMN process ID
   * @param payload initial payload for the instance
   * @return unique ID used to interact with the instance
   */
  public long createWorkflowInstance(String processId, Map<String, Object> payload) {
    return clientRule
        .getClient()
        .newCreateInstanceCommand()
        .bpmnProcessId(processId)
        .latestVersion()
        .payload(payload)
        .send()
        .join()
        .getWorkflowInstanceKey();
  }

  /**
   * Creates a new job worker that will handle jobs of type {@param type}.
   *
   * <p>Make sure to close the returned job worker.
   *
   * @param type type of the jobs to handle
   * @param handler handler
   * @return a new JobWorker
   */
  public JobWorker createJobWorker(String type, JobHandler handler) {
    return clientRule.getClient().newWorker().jobType(type).handler(handler).open();
  }

  /**
   * Publishes a new message to the broker.
   *
   * @param messageName name of the message
   * @param correlationKey correlation key
   */
  public void publishMessage(String messageName, String correlationKey) {
    clientRule
        .getClient()
        .newPublishMessageCommand()
        .messageName(messageName)
        .correlationKey(correlationKey)
        .send()
        .join();
  }

  /**
   * Blocks and wait until the workflow identified by the key has been completed.
   *
   * @param workflowInstanceKey ID of the workflow
   */
  public void awaitWorkflowCompletion(long workflowInstanceKey) {
    TestUtil.waitUntil(
        () ->
            workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_COMPLETED)
                .filter(r -> r.getKey() == workflowInstanceKey)
                .exists());
  }

  private Properties newClientProperties() {
    final Properties properties = new Properties();
    properties.put(
        ClientProperties.BROKER_CONTACTPOINT,
        getBrokerCfg().getGateway().getNetwork().toSocketAddress().toString());

    return properties;
  }

  private void configure(List<ExporterCfg> exporters) {
    configured = true;
    getBrokerCfg().getExporters().addAll(exporters);
  }

  @FunctionalInterface
  public interface RecordVisitor {

    boolean visit(Record<?> record);
  }
}
