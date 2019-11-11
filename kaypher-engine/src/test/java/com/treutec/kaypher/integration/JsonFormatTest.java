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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.common.utils.IntegrationTest;
import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.KaypherConfigTestUtil;
import com.treutec.kaypher.ServiceInfo;
import com.treutec.kaypher.engine.KaypherEngine;
import com.treutec.kaypher.engine.KaypherEngineTestUtil;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.query.id.SequentialQueryIdGenerator;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.PhysicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.services.DisabledKaypherClient;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.services.ServiceContextFactory;
import com.treutec.kaypher.test.util.EmbeddedSingleNodeKafkaCluster;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.OrderDataProvider;
import com.treutec.kaypher.util.PersistentQueryMetadata;
import com.treutec.kaypher.util.QueryMetadata;
import com.treutec.kaypher.util.TopicConsumer;
import com.treutec.kaypher.util.TopicProducer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class JsonFormatTest {
  private static final String inputTopic = "orders_topic";
  private static final String inputStream = "ORDERS";
  private static final String usersTopic = "users_topic";
  private static final String usersTable = "USERS";
  private static final String messageLogTopic = "log_topic";
  private static final String messageLogStream = "message_log";
  private static final AtomicInteger COUNTER = new AtomicInteger();

  private static final EmbeddedSingleNodeKafkaCluster CLUSTER = EmbeddedSingleNodeKafkaCluster.build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(CLUSTER);

  private MetaStore metaStore;
  private KaypherConfig kaypherConfig;
  private KaypherEngine kaypherEngine;
  private ServiceContext serviceContext;
  private final TopicProducer topicProducer = new TopicProducer(CLUSTER);
  private final TopicConsumer topicConsumer = new TopicConsumer(CLUSTER);

  private QueryId queryId;
  private KafkaTopicClient topicClient;
  private String streamName;

  @Before
  public void before() throws Exception {
    streamName = "STREAM_" + COUNTER.getAndIncrement();

    kaypherConfig = KaypherConfigTestUtil.create(CLUSTER);
    serviceContext = ServiceContextFactory.create(kaypherConfig, DisabledKaypherClient.instance());

    kaypherEngine = new KaypherEngine(
        serviceContext,
        ProcessingLogContext.create(),
        new InternalFunctionRegistry(),
        ServiceInfo.create(kaypherConfig),
        new SequentialQueryIdGenerator());

    topicClient = serviceContext.getTopicClient();
    metaStore = kaypherEngine.getMetaStore();

    createInitTopics();
    produceInitData();
    execInitCreateStreamQueries();
  }

  private void createInitTopics() {
    topicClient.createTopic(inputTopic, 1, (short) 1);
    topicClient.createTopic(usersTopic, 1, (short) 1);
    topicClient.createTopic(messageLogTopic, 1, (short) 1);
  }

  private void produceInitData() throws Exception {
    final OrderDataProvider orderDataProvider = new OrderDataProvider();

    topicProducer
            .produceInputData(inputTopic, orderDataProvider.data(), orderDataProvider.schema());

    final LogicalSchema messageSchema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("MESSAGE"), SqlTypes.STRING)
        .build();

    final GenericRow messageRow = new GenericRow(Collections.singletonList(
        "{\"log\":{\"@timestamp\":\"2017-05-30T16:44:22.175Z\",\"@version\":\"1\","
        + "\"caasVersion\":\"0.0.2\",\"cloud\":\"aws\",\"logs\":[{\"entry\":\"first\"}],\"clusterId\":\"cp99\",\"clusterName\":\"kafka\",\"cpComponentId\":\"kafka\",\"host\":\"kafka-1-wwl0p\",\"k8sId\":\"k8s13\",\"k8sName\":\"perf\",\"level\":\"ERROR\",\"logger\":\"kafka.server.ReplicaFetcherThread\",\"message\":\"Found invalid messages during fetch for partition [foo512,172] offset 0 error Record is corrupt (stored crc = 1321230880, computed crc = 1139143803)\",\"networkId\":\"vpc-d8c7a9bf\",\"region\":\"us-west-2\",\"serverId\":\"1\",\"skuId\":\"sku5\",\"source\":\"kafka\",\"tenantId\":\"t47\",\"tenantName\":\"perf-test\",\"thread\":\"ReplicaFetcherThread-0-2\",\"zone\":\"us-west-2a\"},\"stream\":\"stdout\",\"time\":2017}"));

    final Map<String, GenericRow> records = new HashMap<>();
    records.put("1", messageRow);

    final PhysicalSchema schema = PhysicalSchema.from(
        messageSchema,
        SerdeOption.none()
    );

    topicProducer.produceInputData(messageLogTopic, records, schema);
  }

  private void execInitCreateStreamQueries() {
    final String ordersStreamStr = String.format("CREATE STREAM %s (ORDERTIME bigint, ORDERID varchar, "
        + "ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP "
        + "map<varchar, double>) WITH (value_format = 'json', "
        + "kafka_topic='%s' , "
        + "key='ordertime');", inputStream, inputTopic);

    final String usersTableStr = String.format("CREATE TABLE %s (userid varchar, age integer) WITH "
                                         + "(value_format = 'json', kafka_topic='%s', "
                                         + "KEY='userid');",
                                         usersTable, usersTopic);

    final String messageStreamStr = String.format("CREATE STREAM %s (message varchar) WITH (value_format = 'json', "
        + "kafka_topic='%s');", messageLogStream, messageLogTopic);

    KaypherEngineTestUtil.execute(
        serviceContext, kaypherEngine, ordersStreamStr, kaypherConfig, Collections.emptyMap());
    KaypherEngineTestUtil.execute(
        serviceContext, kaypherEngine, usersTableStr, kaypherConfig, Collections.emptyMap());
    KaypherEngineTestUtil.execute(
        serviceContext, kaypherEngine, messageStreamStr, kaypherConfig, Collections.emptyMap());
  }

  @After
  public void after() {
    terminateQuery();
    kaypherEngine.close();
    serviceContext.close();
  }

  //@Test
  public void testSelectDateTimeUDFs() {
    final String streamName = "SelectDateTimeUDFsStream".toUpperCase();

    final String selectColumns =
        "(ORDERTIME+1500962514806) , TIMESTAMPTOSTRING(ORDERTIME+1500962514806, "
        + "'yyyy-MM-dd HH:mm:ss.SSS'), "
        + "STRINGTOTIMESTAMP"
            + "(TIMESTAMPTOSTRING"
            + "(ORDERTIME+1500962514806, 'yyyy-MM-dd HH:mm:ss.SSS'), 'yyyy-MM-dd HH:mm:ss.SSS')";
    final String whereClause = "ORDERUNITS > 20 AND ITEMID LIKE '%_8'";

    final String queryString = String.format(
        "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
        streamName,
        selectColumns,
        inputStream,
        whereClause
    );

    executePersistentQuery(queryString);

    final Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("8", new GenericRow(Arrays.asList(1500962514814L,
        "2017-07-24 23:01:54.814",
        1500962514814L)));

    final Map<String, GenericRow> results = readNormalResults(streamName, expectedResults.size());

    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testJsonStreamExtractor() {
    final String queryString = String.format("CREATE STREAM %s AS SELECT EXTRACTJSONFIELD"
            + "(message, '$.log.cloud') "
            + "FROM %s;",
        streamName, messageLogStream);

    executePersistentQuery(queryString);

    final Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Collections.singletonList("aws")));

    final Map<String, GenericRow> results = readNormalResults(streamName, expectedResults.size());

    assertThat(results, equalTo(expectedResults));
  }

  @Test
  public void testJsonStreamExtractorNested() {
    final String queryString = String.format("CREATE STREAM %s AS SELECT EXTRACTJSONFIELD"
                    + "(message, '$.log.logs[0].entry') "
                    + "FROM %s;",
            streamName, messageLogStream);

    executePersistentQuery(queryString);

    final Map<String, GenericRow> expectedResults = new HashMap<>();
    expectedResults.put("1", new GenericRow(Collections.singletonList("first")));

    final Map<String, GenericRow> results = readNormalResults(streamName, expectedResults.size());

    assertThat(results, equalTo(expectedResults));
  }

  private void executePersistentQuery(final String queryString) {
    final QueryMetadata queryMetadata = KaypherEngineTestUtil
        .execute(serviceContext, kaypherEngine, queryString, kaypherConfig, Collections.emptyMap())
        .get(0);

    queryMetadata.start();
    queryId = ((PersistentQueryMetadata)queryMetadata).getQueryId();
  }

  private Map<String, GenericRow> readNormalResults(
      final String resultTopic,
      final int expectedNumMessages
  ) {
    final DataSource<?> source = metaStore.getSource(SourceName.of(streamName));

    final PhysicalSchema resultSchema = PhysicalSchema.from(
        source.getSchema(),
        source.getSerdeOptions()
    );

    return topicConsumer.readResults(resultTopic, resultSchema, expectedNumMessages, new StringDeserializer());
  }

  private void terminateQuery() {
    kaypherEngine.getPersistentQuery(queryId)
        .ifPresent(QueryMetadata::close);
  }
}
