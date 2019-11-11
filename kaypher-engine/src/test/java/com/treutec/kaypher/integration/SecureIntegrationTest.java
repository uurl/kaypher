/*
 * Copyright 2019 Treu Techologies
 *
 * See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.treutec.kaypher.integration;

import static com.treutec.kaypher.test.util.AssertEventually.assertThatEventually;
import static com.treutec.kaypher.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER1;
import static com.treutec.kaypher.test.util.EmbeddedSingleNodeKafkaCluster.VALID_USER2;
import static com.treutec.kaypher.test.util.EmbeddedSingleNodeKafkaCluster.ops;
import static com.treutec.kaypher.test.util.EmbeddedSingleNodeKafkaCluster.prefixedResource;
import static com.treutec.kaypher.test.util.EmbeddedSingleNodeKafkaCluster.resource;
import static com.treutec.kaypher.util.KaypherConfig.KAYPHER_SERVICE_ID_CONFIG;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import com.treutec.kaypher.KaypherConfigTestUtil;
import com.treutec.kaypher.ServiceInfo;
import com.treutec.kaypher.engine.KaypherEngine;
import com.treutec.kaypher.engine.KaypherEngineTestUtil;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.query.id.SequentialQueryIdGenerator;
import com.treutec.kaypher.services.DisabledKaypherClient;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.services.KafkaTopicClientImpl;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.services.ServiceContextFactory;
import com.treutec.kaypher.test.util.EmbeddedSingleNodeKafkaCluster;
import com.treutec.kaypher.test.util.secure.ClientTrustStore;
import com.treutec.kaypher.test.util.secure.Credentials;
import com.treutec.kaypher.test.util.secure.SecureKafkaHelper;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.OrderDataProvider;
import com.treutec.kaypher.util.PersistentQueryMetadata;
import com.treutec.kaypher.util.QueryMetadata;
import com.treutec.kaypher.util.TopicConsumer;
import com.treutec.kaypher.util.TopicProducer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import kafka.security.auth.Acl;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

/**
 * Tests covering integration with secured components, e.g. secure Kafka cluster.
 */
@Category({IntegrationTest.class})
public class SecureIntegrationTest {

  private static final String INPUT_TOPIC = "orders_topic";
  private static final String INPUT_STREAM = "ORDERS";
  private static final Credentials ALL_USERS = new Credentials("*", "ignored");
  private static final Credentials SUPER_USER = VALID_USER1;
  private static final Credentials NORMAL_USER = VALID_USER2;
  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  private static final EmbeddedSingleNodeKafkaCluster SECURE_CLUSTER =
      EmbeddedSingleNodeKafkaCluster.newBuilder()
          .withoutPlainListeners()
          .withSaslSslListeners()
          .withSslListeners()
          .withAclsEnabled(SUPER_USER.username)
          .build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(SECURE_CLUSTER);

  private QueryId queryId;
  private KaypherConfig kaypherConfig;
  private KaypherEngine kaypherEngine;
  private final TopicProducer topicProducer = new TopicProducer(SECURE_CLUSTER);
  private KafkaTopicClient topicClient;
  private String outputTopic;
  private AdminClient adminClient;
  private ServiceContext serviceContext;

  @Before
  public void before() throws Exception {
    SECURE_CLUSTER.clearAcls();
    outputTopic = "TEST_" + COUNTER.incrementAndGet();

    adminClient = AdminClient
        .create(new KaypherConfig(getKaypherConfig(SUPER_USER)).getKaypherAdminClientConfigProps());
    topicClient = new KafkaTopicClientImpl(
        adminClient);

    produceInitData();
  }

  @After
  public void after() {
    if (queryId != null) {
      kaypherEngine.getPersistentQuery(queryId)
          .ifPresent(QueryMetadata::close);
    }
    if (kaypherEngine != null) {
      kaypherEngine.close();
    }
    if (topicClient != null) {
      try {
        topicClient.deleteTopics(Collections.singletonList(outputTopic));
      } catch (final Exception e) {
        e.printStackTrace(System.err);
      }
      adminClient.close();
    }
    if (serviceContext != null) {
      serviceContext.close();
    }
  }

  @Test
  public void shouldRunQueryAgainstKafkaClusterOverSsl() throws Exception {
    // Given:
    givenAllowAcl(ALL_USERS,
                  resource(CLUSTER, "kafka-cluster"),
                  ops(DESCRIBE_CONFIGS, CREATE));

    givenAllowAcl(ALL_USERS,
                  resource(TOPIC, Acl.WildCardResource()),
                  ops(DESCRIBE, READ, WRITE, DELETE));

    givenAllowAcl(ALL_USERS,
                  resource(GROUP, Acl.WildCardResource()),
                  ops(DESCRIBE, READ));

    final Map<String, Object> configs = getBaseKaypherConfig();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                SECURE_CLUSTER.bootstrapServers(SecurityProtocol.SSL));

    // Additional Properties required for KAYPHER to talk to cluster over SSL:
    configs.put("security.protocol", "SSL");
    configs.put("ssl.truststore.location", ClientTrustStore.trustStorePath());
    configs.put("ssl.truststore.password", ClientTrustStore.trustStorePassword());

    givenTestSetupWithConfig(configs);

    // Then:
    assertCanRunSimpleKaypherQuery();
  }

  @Test
  public void shouldRunQueryAgainstKafkaClusterOverSaslSsl() throws Exception {
    // Given:
    final Map<String, Object> configs = getBaseKaypherConfig();

    // Additional Properties required for KAYPHER to talk to secure cluster using SSL and SASL:
    configs.put("security.protocol", "SASL_SSL");
    configs.put("sasl.mechanism", "PLAIN");
    configs.put("sasl.jaas.config", SecureKafkaHelper.buildJaasConfig(SUPER_USER));

    givenTestSetupWithConfig(configs);

    // Then:
    assertCanRunSimpleKaypherQuery();
  }

  @Test
  public void shouldRunQueriesRequiringChangeLogsAndRepartitionTopicsWithMinimalPrefixedAcls()
      throws Exception {

    final String serviceId = "my-service-id_";  // Defaults to "default_"

    final Map<String, Object> kaypherConfig = getKaypherConfig(NORMAL_USER);
    kaypherConfig.put(KAYPHER_SERVICE_ID_CONFIG, serviceId);

    givenAllowAcl(NORMAL_USER,
                  resource(CLUSTER, "kafka-cluster"),
                  ops(DESCRIBE_CONFIGS));

    givenAllowAcl(NORMAL_USER,
                  resource(TOPIC, INPUT_TOPIC),
                  ops(READ));

    givenAllowAcl(NORMAL_USER,
                  resource(TOPIC, outputTopic),
                  ops(CREATE /* as the topic doesn't exist yet*/, WRITE));

    givenAllowAcl(NORMAL_USER,
                  prefixedResource(TOPIC, "_confluent-kaypher-my-service-id_"),
                  ops(ALL));

    givenAllowAcl(NORMAL_USER,
                  prefixedResource(GROUP, "_confluent-kaypher-my-service-id_"),
                  ops(ALL));

    givenTestSetupWithConfig(kaypherConfig);

    // Then:
    assertCanRunRepartitioningKaypherQuery();
  }

  // Requires correctly configured schema-registry running
  //@Test
  @SuppressWarnings("unused")
  public void shouldRunQueryAgainstSecureSchemaRegistry() throws Exception {
    // Given:
    final HostnameVerifier existing = HttpsURLConnection.getDefaultHostnameVerifier();
    HttpsURLConnection.setDefaultHostnameVerifier(
        (hostname, sslSession) -> hostname.equals("localhost"));

    try {
      final Map<String, Object> kaypherConfig = getKaypherConfig(SUPER_USER);
      kaypherConfig.put("kaypher.schema.registry.url", "https://localhost:8481");
      kaypherConfig.put("ssl.truststore.location", ClientTrustStore.trustStorePath());
      kaypherConfig.put("ssl.truststore.password", ClientTrustStore.trustStorePassword());
      givenTestSetupWithConfig(kaypherConfig);

      // Then:
      assertCanRunKaypherQuery("CREATE STREAM %s WITH (VALUE_FORMAT='AVRO') AS "
                            + "SELECT * FROM %s;",
                            outputTopic, INPUT_STREAM);
    } finally {
      HttpsURLConnection.setDefaultHostnameVerifier(existing);
    }
  }

  private static void givenAllowAcl(final Credentials credentials,
                                    final ResourcePattern resource,
                                    final Set<AclOperation> ops) {
    SECURE_CLUSTER.addUserAcl(credentials.username, AclPermissionType.ALLOW, resource, ops);
  }

  private void givenTestSetupWithConfig(final Map<String, Object> kaypherConfigs) {
    kaypherConfig = new KaypherConfig(kaypherConfigs);
    serviceContext = ServiceContextFactory.create(kaypherConfig, DisabledKaypherClient.instance());
    kaypherEngine = new KaypherEngine(
        serviceContext,
        ProcessingLogContext.create(),
        new InternalFunctionRegistry(),
        ServiceInfo.create(kaypherConfig),
        new SequentialQueryIdGenerator());

    execInitCreateStreamQueries();
  }

  private void assertCanRunSimpleKaypherQuery() throws Exception {
    assertCanRunKaypherQuery("CREATE STREAM %s AS SELECT * FROM %s;",
                          outputTopic, INPUT_STREAM);
  }

  private void assertCanRunRepartitioningKaypherQuery() throws Exception {
    assertCanRunKaypherQuery("CREATE TABLE %s AS SELECT itemid, count(*) "
                          + "FROM %s WINDOW TUMBLING (size 5 second) GROUP BY itemid;",
                          outputTopic, INPUT_STREAM);
  }

  private void assertCanRunKaypherQuery(
      final String queryString,
      final Object... args
  ) {
    executePersistentQuery(queryString, args);

    assertThatEventually(
        "Wait for async topic creation",
        () -> topicClient.isTopicExists(outputTopic),
        is(true)
    );

    final TopicConsumer consumer = new TopicConsumer(SECURE_CLUSTER);
    consumer.verifyRecordsReceived(outputTopic, greaterThan(0));
  }

  private static Map<String, Object> getBaseKaypherConfig() {
    final Map<String, Object> configs = new HashMap<>(KaypherConfigTestUtil.baseTestConfig());
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SECURE_CLUSTER.bootstrapServers());

    // Additional Properties required for KAYPHER to talk to test secure cluster,
    // where SSL cert not properly signed. (Not required for proper cluster).
    configs.putAll(ClientTrustStore.trustStoreProps());
    return configs;
  }

  private static Map<String, Object> getKaypherConfig(final Credentials user) {
    final Map<String, Object> configs = getBaseKaypherConfig();
    configs.putAll(SecureKafkaHelper.getSecureCredentialsConfig(user));
    return configs;
  }

  private void produceInitData() throws Exception {
    if (topicClient.isTopicExists(INPUT_TOPIC)) {
      return;
    }

    topicClient.createTopic(INPUT_TOPIC, 1, (short) 1);

    awaitAsyncInputTopicCreation();

    final OrderDataProvider orderDataProvider = new OrderDataProvider();

    topicProducer
        .produceInputData(INPUT_TOPIC, orderDataProvider.data(), orderDataProvider.schema());
  }

  private void awaitAsyncInputTopicCreation() {
    assertThatEventually(() -> topicClient.isTopicExists(INPUT_TOPIC), is(true));
  }

  private void execInitCreateStreamQueries() {
    final String ordersStreamStr = String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, "
                                           + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
                                           + "map<varchar, double>) WITH (value_format = 'json', "
                                           + "kafka_topic='%s' , "
                                           + "key='ordertime');", INPUT_STREAM, INPUT_TOPIC);

    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        ordersStreamStr,
        kaypherConfig,
        Collections.emptyMap()
    );
  }

  private void executePersistentQuery(final String queryString,
                                      final Object... params) {
    final String query = String.format(queryString, params);

    final QueryMetadata queryMetadata = KaypherEngineTestUtil
        .execute(serviceContext, kaypherEngine, query, kaypherConfig, Collections.emptyMap()).get(0);

    queryMetadata.start();
    queryId = ((PersistentQueryMetadata) queryMetadata).getQueryId();
  }
}
