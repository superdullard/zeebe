package io.zeebe.broker.workflow.processor;


public class DeploymentCreatedProcessorTest {

//  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
//
//  public ClientApiRule apiRule = new ClientApiRule(brokerRule::getClientAddress);
//
//  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);
//
//  private PartitionTestClient testClient;
//
//  @Before
//  public void init() {
//    testClient = apiRule.partitionClient();
//  }
//
//  @Test
//  public void shouldOpenMessageSubscriptionOnDeployment() {
//
//    testClient.deploy(createWorkflowWithOneMessageStartEvent());
//
//    //TODO
//    assertThat(
//      RecordingExporter.
//        messageSubscriptionRecords(MessageSubscriptionIntent.OPENED).exists())
//      .isTrue();
//    // then
//    waitUntil(
//        () ->
//            rule.events()
//                .onlyMessageStartEventSubscriptionRecords()
//                .withIntent(MessageSubscriptionIntent.OPEN)
//                .findFirst()
//                .isPresent());
//
//    final TypedRecord<MessageStartEventSubscriptionRecord> subscription =
//        rule.events()
//            .onlyMessageStartEventSubscriptionRecords()
//            .withIntent(MessageSubscriptionIntent.OPEN)
//            .findFirst()
//            .get();
//
//    assertThat(subscription.getValue().getMessageName()).isEqualTo(wrapString("startMessage"));
//    assertThat(subscription.getValue().getStartEventId()).isEqualTo(wrapString("startEventId"));
//    assertThat(subscription.getValue().getWorkflowProcessId()).isEqualTo(wrapString("processId"));
//  }
//
//  @Test
//  public void shouldOpenSubscriptionsForAllMessageStartEvents() {
//    streamProcessor.start();
//
//    // when
//    final DeploymentRecord deploymentRecord = createWorkflowWithTwoMessageStartEvent();
//    rule.writeCommand(DeploymentIntent.CREATE, deploymentRecord);
//
//    // then
//    waitUntil(
//        () ->
//            rule.events()
//                    .onlyMessageStartEventSubscriptionRecords()
//                    .withIntent(MessageSubscriptionIntent.OPEN)
//                    .count()
//                == 2);
//
//    List<TypedRecord<MessageStartEventSubscriptionRecord>> subscriptions =
//        rule.events()
//            .onlyMessageStartEventSubscriptionRecords()
//            .withIntent(MessageSubscriptionIntent.OPEN)
//            .asList();
//
//    assertThat(subscriptions.size()).isEqualTo(2);
//
//    TypedRecord<MessageStartEventSubscriptionRecord> firstSubscription = subscriptions.get(0);
//    TypedRecord<MessageStartEventSubscriptionRecord> secondSubscription = subscriptions.get(1);
//
//    assertThat(firstSubscription.getValue().getMessageName())
//        .isEqualTo(wrapString("startMessage2"));
//    assertThat(firstSubscription.getValue().getStartEventId())
//        .isEqualTo(wrapString("startEventId2"));
//    assertThat(firstSubscription.getValue().getWorkflowProcessId())
//        .isEqualTo(wrapString("processId"));
//
//    assertThat(secondSubscription.getValue().getMessageName())
//        .isEqualTo(wrapString("startMessage1"));
//    assertThat(secondSubscription.getValue().getStartEventId())
//        .isEqualTo(wrapString("startEventId1"));
//    assertThat(secondSubscription.getValue().getWorkflowProcessId())
//        .isEqualTo(wrapString("processId"));
//  }
//
//  private static BpmnModelInstance createWorkflowWithOneMessageStartEvent() {
//    final BpmnModelInstance modelInstance =
//        Bpmn.createExecutableProcess("processId")
//            .startEvent("startEventId")
//            .message(m -> m.name("startMessage").id("startmsgId"))
//            .endEvent()
//            .done();
//
//    return modelInstance;
//  }
//
//  private static BpmnModelInstance createWorkflowWithTwoMessageStartEvent() {
//    ProcessBuilder process = Bpmn.createExecutableProcess("processId");
//    process
//        .startEvent("startEventId1")
//        .message(m -> m.name("startMessage1").id("startmsgId"))
//        .endEvent();
//    process
//        .startEvent("startEventId2")
//        .message(m -> m.name("startMessage2").id("startmsgId2"))
//        .endEvent();
//
//    final BpmnModelInstance modelInstance = process.done();
//    return modelInstance;
//  }
}
