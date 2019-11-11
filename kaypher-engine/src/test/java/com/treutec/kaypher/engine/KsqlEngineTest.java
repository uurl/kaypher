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
package com.treutec.kaypher.engine;

import static com.treutec.kaypher.metastore.model.MetaStoreMatchers.FieldMatchers.hasFullName;
import static com.treutec.kaypher.util.KaypherExceptionMatcher.rawMessage;
import static com.treutec.kaypher.util.KaypherExceptionMatcher.statementText;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import com.treutec.kaypher.KaypherConfigTestUtil;
import com.treutec.kaypher.KaypherExecutionContext;
import com.treutec.kaypher.KaypherExecutionContext.ExecuteResult;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.KaypherParser.ParsedStatement;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.exception.ParseFailedException;
import com.treutec.kaypher.parser.tree.CreateStream;
import com.treutec.kaypher.parser.tree.CreateStreamAsSelect;
import com.treutec.kaypher.parser.tree.CreateTable;
import com.treutec.kaypher.parser.tree.DropTable;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.services.FakeKafkaTopicClient;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.services.TestServiceContext;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherConstants;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.KaypherStatementException;
import com.treutec.kaypher.util.MetaStoreFixture;
import com.treutec.kaypher.util.PersistentQueryMetadata;
import com.treutec.kaypher.util.QueryMetadata;
import com.treutec.kaypher.util.SchemaUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "SameParameterValue"})
@RunWith(MockitoJUnitRunner.class)
public class KaypherEngineTest {

  private static final KaypherConfig KAYPHER_CONFIG = KaypherConfigTestUtil.create("what-eva");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private MutableMetaStore metaStore;
  @Spy
  private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
  private final Supplier<SchemaRegistryClient> schemaRegistryClientFactory =
      () -> schemaRegistryClient;

  private KaypherEngine kaypherEngine;
  private ServiceContext serviceContext;
  private ServiceContext sandboxServiceContext;
  @Spy
  private final FakeKafkaTopicClient topicClient = new FakeKafkaTopicClient();
  private KaypherExecutionContext sandbox;

  @Before
  public void setUp() {
    metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

    serviceContext = TestServiceContext.create(
        topicClient,
        schemaRegistryClientFactory
    );

    kaypherEngine = KaypherEngineTestUtil.createKaypherEngine(
        serviceContext,
        metaStore
    );

    sandbox = kaypherEngine.createSandbox(serviceContext);
    sandboxServiceContext = sandbox.getServiceContext();
  }

  @After
  public void closeEngine() {
    kaypherEngine.close();
    serviceContext.close();
  }

  @Test
  public void shouldCreatePersistentQueries() {
    // When:
    final List<QueryMetadata> queries
        = KaypherEngineTestUtil.execute(
            serviceContext,
        kaypherEngine,
        "create table bar as select * from test2;"
            + "create table foo as select * from test2;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(queries, hasSize(2));
    assertThat(queries.get(0), is(instanceOf(PersistentQueryMetadata.class)));
    assertThat(queries.get(1), is(instanceOf(PersistentQueryMetadata.class)));
    assertThat(((PersistentQueryMetadata) queries.get(0)).getSinkName(), is(SourceName.of("BAR")));
    assertThat(((PersistentQueryMetadata) queries.get(1)).getSinkName(), is(SourceName.of("FOO")));
  }

  @Test
  public void shouldNotHaveRowTimeAndRowKeyColumnsInPersistentQueryValueSchema() {
    // When:
    final PersistentQueryMetadata query = (PersistentQueryMetadata) KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create table bar as select * from test2;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    ).get(0);

    // Then:
    assertThat(query.getLogicalSchema().value(),
        not(hasItem(hasFullName(SchemaUtil.ROWTIME_NAME))));

    assertThat(query.getLogicalSchema().value(),
        not(hasItem(hasFullName(SchemaUtil.ROWKEY_NAME))));
  }

  @Test
  public void shouldThrowOnTerminateAsNotExecutable() {
    // Given:
    final PersistentQueryMetadata query = (PersistentQueryMetadata) KaypherEngineTestUtil
        .execute(
            serviceContext,
            kaypherEngine,
            "create table bar as select * from test2;",
            KAYPHER_CONFIG,
            Collections.emptyMap())
        .get(0);

    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(is("Statement not executable")));
    expectedException.expect(statementText(is("TERMINATE CTAS_BAR_0;")));

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "TERMINATE " + query.getQueryId() + ";",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldExecuteInsertIntoStreamOnSandBox() {
    // Given:
    final List<ParsedStatement> statements = parse(
        "create stream bar as select * from orders;"
            + "insert into bar select * from orders;"
    );

    givenStatementAlreadyExecuted(statements.get(0));

    // When:
    final ExecuteResult result = sandbox
        .execute(sandboxServiceContext, ConfiguredStatement.of(
            sandbox.prepare(statements.get(1)),
            Collections.emptyMap(),
            KAYPHER_CONFIG));

    // Then:
    assertThat(result.getQuery(), is(not(Optional.empty())));
  }

  @Test
  public void shouldThrowWhenExecutingInsertIntoTable() {
    KaypherEngineTestUtil.execute(
        serviceContext, kaypherEngine, "create table bar as select * from test2;", KAYPHER_CONFIG,
        Collections.emptyMap());

    final ParsedStatement parsed = kaypherEngine.parse("insert into bar select * from test2;").get(0);

    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "INSERT INTO can only be used to insert into a stream. BAR is a table.")));
    expectedException.expect(statementText(is("insert into bar select * from test2;")));

    // When:
    prepare(parsed);
  }

  @Test
  public void shouldThrowOnInsertIntoStreamWithTableResult() {
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create stream bar as select itemid, orderid from orders;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "Incompatible data sink and query result. "
            + "Data sink (BAR) type is KSTREAM but select query result is KTABLE.")));
    expectedException.expect(statementText(
        is("insert into bar select itemid, count(*) from orders group by itemid;")));

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "insert into bar select itemid, count(*) from orders group by itemid;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldThrowOnInsertIntoWithKeyMismatch() {
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create stream bar as select * from orders;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "Incompatible key fields for sink and results. "
            + "Sink key field is ORDERTIME (type: BIGINT) "
            + "while result key field is ITEMID (type: STRING)")));
    expectedException.expect(statementText(
        is("insert into bar select * from orders partition by itemid;")));

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "insert into bar select * from orders partition by itemid;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldThrowWhenInsertIntoSchemaDoesNotMatch() {
    // Given:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create stream bar as select * from orders;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "Incompatible schema between results and sink.")));
    expectedException.expect(statementText(is("insert into bar select itemid from orders;")));

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "insert into bar select itemid from orders;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldExecuteInsertIntoStream() {
    // Given:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create stream bar as select * from orders;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    // When:
    final List<QueryMetadata> queries = KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "insert into bar select * from orders;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(queries, hasSize(1));
  }

  @Test
  public void shouldMaintainOrderOfReturnedQueries() {
    // When:
    final List<QueryMetadata> queries = KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create stream foo as select * from orders;"
            + "create stream bar as select * from orders;",
        KAYPHER_CONFIG, Collections.emptyMap());

    // Then:
    assertThat(queries, hasSize(2));
    assertThat(queries.get(0).getStatementString(), containsString("CREATE STREAM FOO"));
    assertThat(queries.get(1).getStatementString(), containsString("CREATE STREAM BAR"));
  }

  @Test(expected = KaypherStatementException.class)
  public void shouldFailToCreateQueryIfSelectingFromNonExistentEntity() {
    KaypherEngineTestUtil
        .execute(
            serviceContext,
            kaypherEngine,
            "select * from bar;",
            KAYPHER_CONFIG,
            Collections.emptyMap()
        );
  }

  @Test(expected = ParseFailedException.class)
  public void shouldFailWhenSyntaxIsInvalid() {
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "blah;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldUpdateReferentialIntegrityTableCorrectly() {
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create table bar as select * from test2;"
            + "create table foo as select * from test2;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    assertThat(metaStore.getQueriesWithSource(SourceName.of("TEST2")),
        equalTo(Utils.mkSet("CTAS_BAR_0", "CTAS_FOO_1")));
    assertThat(metaStore.getQueriesWithSink(SourceName.of("BAR")), equalTo(Utils.mkSet("CTAS_BAR_0")));
    assertThat(metaStore.getQueriesWithSink(SourceName.of("FOO")), equalTo(Utils.mkSet("CTAS_FOO_1")));
  }

  @Test
  public void shouldFailIfReferentialIntegrityIsViolated() {
    // Given:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create table bar as select * from test2;"
            + "create table foo as select * from test2;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(is(
        "Cannot drop FOO.\n"
            + "The following queries read from this source: [].\n"
            + "The following queries write into this source: [CTAS_FOO_1].\n"
            + "You need to terminate them before dropping FOO.")));
    expectedException.expect(statementText(is("drop table foo;")));

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "drop table foo;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldFailDDLStatementIfTopicDoesNotExist() {
    // Given:
    final ParsedStatement stmt = parse(
        "CREATE STREAM S1_NOTEXIST (COL1 BIGINT, COL2 VARCHAR) "
            + "WITH  (KAFKA_TOPIC = 'S1_NOTEXIST', VALUE_FORMAT = 'JSON');").get(0);

    final PreparedStatement<?> prepared = prepare(stmt);

    // Then:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expectMessage("Kafka topic does not exist: S1_NOTEXIST");

    // When:
    sandbox.execute(
        sandboxServiceContext,
        ConfiguredStatement.of(prepared,  Collections.emptyMap(), KAYPHER_CONFIG)
    );
  }

  @Test
  public void shouldDropTableIfAllReferencedQueriesTerminated() {
    // Given:
    final QueryMetadata secondQuery = KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create table bar as select * from test2;"
            + "create table foo as select * from test2;",
        KAYPHER_CONFIG,
        Collections.emptyMap())
        .get(1);

    secondQuery.close();

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "drop table foo;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(metaStore.getSource(SourceName.of("foo")), nullValue());
  }

  @Test
  public void shouldNotEnforceTopicExistenceWhileParsing() {
    final String runScriptContent = "CREATE STREAM S1 (COL1 BIGINT, COL2 VARCHAR) "
        + "WITH  (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');\n"
        + "CREATE TABLE T1 AS SELECT COL1, count(*) FROM "
        + "S1 GROUP BY COL1;\n"
        + "CREATE STREAM S2 (C1 BIGINT, C2 BIGINT) "
        + "WITH (KAFKA_TOPIC = 'T1', VALUE_FORMAT = 'JSON');\n";

    final List<?> parsedStatements = kaypherEngine.parse(runScriptContent);

    assertThat(parsedStatements.size(), equalTo(3));
  }

  @Test
  public void shouldThrowFromSandBoxOnPrepareIfSourceTopicDoesNotExist() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE STREAM S1 (COL1 BIGINT) "
            + "WITH (KAFKA_TOPIC = 'i_do_not_exist', VALUE_FORMAT = 'JSON');").get(0));

    // Expect:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(is(
        "Kafka topic does not exist: i_do_not_exist")));
    expectedException.expect(statementText(is(
        "CREATE STREAM S1 (COL1 BIGINT)"
            + " WITH (KAFKA_TOPIC = 'i_do_not_exist', VALUE_FORMAT = 'JSON');")));

    // When:
    sandbox.execute(
        sandboxServiceContext,
        ConfiguredStatement.of(statement, new HashMap<>(), KAYPHER_CONFIG)
    );
  }

  @Test
  public void shouldThrowFromExecuteIfSourceTopicDoesNotExist() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE STREAM S1 (COL1 BIGINT) "
            + "WITH (KAFKA_TOPIC = 'i_do_not_exist', VALUE_FORMAT = 'JSON');").get(0));

    // Expect:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(is("Kafka topic does not exist: i_do_not_exist")));

    // When:
    kaypherEngine.execute(
        serviceContext,
        ConfiguredStatement.of(statement, new HashMap<>(), KAYPHER_CONFIG)
    );
  }

  @Test
  public void shouldHandleCommandsSpreadOverMultipleLines() {
    final String runScriptContent = "CREATE STREAM S1 \n"
        + "(COL1 BIGINT, COL2 VARCHAR)\n"
        + " WITH \n"
        + "(KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');\n";

    final List<?> parsedStatements = kaypherEngine.parse(runScriptContent);

    assertThat(parsedStatements, hasSize(1));
  }

  @Test
  public void shouldThrowIfSchemaNotPresent() {
    // Given:
    givenTopicsExist("bar");

    // Then:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "The statement does not define any columns.")));
    expectedException.expect(statementText(is(
        "create stream bar with (value_format='avro', kafka_topic='bar');")));

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create stream bar with (value_format='avro', kafka_topic='bar');",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldFailIfAvroSchemaNotEvolvable() {
    // Given:
    givenTopicWithSchema("T", Schema.create(Type.INT));

    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "Cannot register avro schema for T as the schema is incompatible with the current schema version registered for the topic.\n" +
            "KAYPHER schema: {" +
            "\"type\":\"record\"," +
            "\"name\":\"KaypherDataSourceSchema\"," +
            "\"namespace\":\"com.treutec.kaypher.avro_schemas\"," +
            "\"fields\":[" +
            "{\"name\":\"COL0\",\"type\":[\"null\",\"long\"],\"default\":null}," +
            "{\"name\":\"COL1\",\"type\":[\"null\",\"string\"],\"default\":null}," +
            "{\"name\":\"COL2\",\"type\":[\"null\",\"string\"],\"default\":null}," +
            "{\"name\":\"COL3\",\"type\":[\"null\",\"double\"],\"default\":null}," +
            "{\"name\":\"COL4\",\"type\":[\"null\",\"boolean\"],\"default\":null}" +
            "],\"connect.name\":\"com.treutec.kaypher.avro_schemas.KaypherDataSourceSchema\"" +
            "}\n" +
            "Registered schema: \"int\"")));
    expectedException.expect(statementText(is(
        "CREATE TABLE T WITH(VALUE_FORMAT='AVRO') AS SELECT * FROM TEST2;")));

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "CREATE TABLE T WITH(VALUE_FORMAT='AVRO') AS SELECT * FROM TEST2;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldNotFailIfAvroSchemaEvolvable() {
    // Given:
    final Schema evolvableSchema = SchemaBuilder
        .record("Test").fields()
        .nullableInt("f1", 1)
        .endRecord();

    givenTopicWithSchema("T", evolvableSchema);

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "CREATE TABLE T WITH(VALUE_FORMAT='AVRO') AS SELECT * FROM TEST2;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(metaStore.getSource(SourceName.of("T")), is(notNullValue()));
  }

  @Test
  public void shouldNotDeleteSchemaNorTopicForTable() throws Exception {
    // Given:
    givenTopicsExist("BAR");
    final QueryMetadata query = KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create table bar with (value_format = 'avro') as select * from test2;",
        KAYPHER_CONFIG, Collections.emptyMap()
    ).get(0);

    query.close();

    final Schema schema = SchemaBuilder
        .record("Test").fields()
        .name("clientHash").type().fixed("MD5").size(16).noDefault()
        .endRecord();

    schemaRegistryClient.register("BAR-value", schema);

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "DROP TABLE bar;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(serviceContext.getTopicClient().isTopicExists("BAR"), equalTo(true));
    assertThat(schemaRegistryClient.getAllSubjects(), hasItem("BAR-value"));
  }

  @Test
  public void shouldCleanUpInternalTopicsOnClose() {
    // Given:
    final QueryMetadata query = KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "select * from test1 EMIT CHANGES;",
        KAYPHER_CONFIG, Collections.emptyMap()
    ).get(0);

    query.start();

    // When:
    query.close();

    // Then:
    verify(topicClient).deleteInternalTopics(query.getQueryApplicationId());
  }

  @Test
  public void shouldNotCleanUpInternalTopicsOnCloseIfQueryNeverStarted() {
    // Given:
    final QueryMetadata query = KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create stream s1 with (value_format = 'avro') as select * from test1;",
        KAYPHER_CONFIG, Collections.emptyMap()
    ).get(0);

    // When:
    query.close();

    // Then:
    verify(topicClient, never()).deleteInternalTopics(any());
  }

  @Test
  public void shouldRemovePersistentQueryFromEngineWhenClosed() {
    // Given:
    final int startingLiveQueries = kaypherEngine.numberOfLiveQueries();
    final int startingPersistentQueries = kaypherEngine.getPersistentQueries().size();

    final QueryMetadata query = KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create stream s1 with (value_format = 'avro') as select * from test1;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    ).get(0);

    // When:
    query.close();

    // Then:
    assertThat(kaypherEngine.getPersistentQuery(getQueryId(query)), is(Optional.empty()));
    assertThat(kaypherEngine.numberOfLiveQueries(), is(startingLiveQueries));
    assertThat(kaypherEngine.getPersistentQueries().size(), is(startingPersistentQueries));
  }

  @Test
  public void shouldRemoveTransientQueryFromEngineWhenClosed() {
    // Given:
    final int startingLiveQueries = kaypherEngine.numberOfLiveQueries();

    final QueryMetadata query = KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "select * from test1 EMIT CHANGES;",
        KAYPHER_CONFIG, Collections.emptyMap()
    ).get(0);

    // When:
    query.close();

    // Then:
    assertThat(kaypherEngine.numberOfLiveQueries(), is(startingLiveQueries));
  }

  @Test
  public void shouldSetKaypherSinkForSinks() {
    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "create stream s as select * from orders;"
            + "create table t as select itemid, count(*) from orders group by itemid;",
        KAYPHER_CONFIG, Collections.emptyMap()
    );

    // Then:
    assertThat(metaStore.getSource(SourceName.of("S")).getKaypherTopic().isKaypherSink(), is(true));
    assertThat(metaStore.getSource(SourceName.of("T")).getKaypherTopic().isKaypherSink(), is(true));
  }

  @Test
  public void shouldThrowIfLeftTableNotJoiningOnTableKey() {

  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldHandleMultipleStatements() {
    // Given:
    final String sql = ""
        + "-- single line comment\n"
        + "/*\n"
        + "   Multi-line comment\n"
        + "*/\n"
        + "CREATE STREAM S0 (a INT, b VARCHAR) "
        + "      WITH (kafka_topic='s0_topic', value_format='DELIMITED');\n"
        + "\n"
        + "CREATE TABLE T1 (f0 BIGINT, f1 DOUBLE) "
        + "     WITH (kafka_topic='t1_topic', value_format='JSON', key = 'f0');\n"
        + "\n"
        + "CREATE STREAM S1 AS SELECT * FROM S0;\n"
        + "\n"
        + "CREATE STREAM S2 AS SELECT * FROM S0;\n"
        + "\n"
        + "DROP TABLE T1;";

    givenTopicsExist("s0_topic", "t1_topic");

    final List<QueryMetadata> queries = new ArrayList<>();

    // When:
    final List<PreparedStatement<?>> preparedStatements = kaypherEngine.parse(sql).stream()
        .map(stmt ->
        {
          final PreparedStatement<?> prepared = kaypherEngine.prepare(stmt);
          final ExecuteResult result = kaypherEngine.execute(
              serviceContext,
              ConfiguredStatement.of(prepared, new HashMap<>(), KAYPHER_CONFIG));
          result.getQuery().ifPresent(queries::add);
          return prepared;
        })
        .collect(Collectors.toList());

    // Then:
    final List<?> statements = preparedStatements.stream()
        .map(PreparedStatement::getStatement)
        .collect(Collectors.toList());

    assertThat(statements, contains(
        instanceOf(CreateStream.class),
        instanceOf(CreateTable.class),
        instanceOf(CreateStreamAsSelect.class),
        instanceOf(CreateStreamAsSelect.class),
        instanceOf(DropTable.class)
    ));

    assertThat(queries, hasSize(2));
  }

  @Test
  public void shouldNotThrowWhenPreparingDuplicateTable() {
    // Given:
    final List<ParsedStatement> parsed = kaypherEngine.parse(
        "CREATE TABLE FOO AS SELECT * FROM TEST2; "
            + "CREATE TABLE FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM TEST2;");

    givenStatementAlreadyExecuted(parsed.get(0));

    // When:
    kaypherEngine.prepare(parsed.get(1));

    // Then: no exception thrown
  }

  @Test
  public void shouldThrowWhenExecutingDuplicateTable() {
    // Given:
    final List<ParsedStatement> parsed = kaypherEngine.parse(
        "CREATE TABLE FOO AS SELECT * FROM TEST2; "
            + "CREATE TABLE FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM TEST2;");

    givenStatementAlreadyExecuted(parsed.get(0));

    final PreparedStatement<?> prepared = prepare(parsed.get(1));

    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(is(
        "Cannot add table 'FOO': A table with the same name already exists")));
    expectedException.expect(statementText(is(
        "CREATE TABLE FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM TEST2;")));

    // When:
    kaypherEngine.execute(
        serviceContext,
        ConfiguredStatement.of(prepared, new HashMap<>(), KAYPHER_CONFIG)
    );
  }

  @Test
  public void shouldThrowWhenPreparingUnknownSource() {
    // Given:
    final ParsedStatement stmt = kaypherEngine.parse(
        "CREATE STREAM FOO AS SELECT * FROM UNKNOWN;").get(0);

    // Then:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expectMessage("UNKNOWN does not exist.");
    expectedException.expect(statementText(is(
        "CREATE STREAM FOO AS SELECT * FROM UNKNOWN;")));

    // When:
    kaypherEngine.prepare(stmt);
  }

  @Test
  public void shouldNotThrowWhenPreparingDuplicateStream() {
    // Given:
    final ParsedStatement stmt = kaypherEngine.parse(
        "CREATE STREAM FOO AS SELECT * FROM ORDERS; "
            + "CREATE STREAM FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS;").get(0);

    // When:
    kaypherEngine.prepare(stmt);

    // Then: No exception thrown.
  }

  @Test
  public void shouldThrowWhenExecutingDuplicateStream() {
    // Given:
    final List<ParsedStatement> parsed = kaypherEngine.parse(
        "CREATE STREAM FOO AS SELECT * FROM ORDERS; "
            + "CREATE STREAM FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS;");

    givenStatementAlreadyExecuted(parsed.get(0));

    final PreparedStatement<?> prepared = kaypherEngine.prepare(parsed.get(1));

    // Then:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(is(
        "Cannot add stream 'FOO': A stream with the same name already exists")));
    expectedException.expect(statementText(is(
        "CREATE STREAM FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS;")));

    // When:
    kaypherEngine.execute(
        serviceContext,
        ConfiguredStatement.of(prepared, new HashMap<>(), KAYPHER_CONFIG)
    );
  }

  @Test
  public void shouldThrowWhenExecutingQueriesIfCsasCreatesTable() {
    // Given:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "Invalid result type. Your SELECT query produces a TABLE. "
            + "Please use CREATE TABLE AS SELECT statement instead.")));
    expectedException.expect(statementText(is(
        "CREATE STREAM FOO AS SELECT COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;")));

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "CREATE STREAM FOO AS SELECT COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;",
        KAYPHER_CONFIG, Collections.emptyMap()
    );
  }

  @Test
  public void shouldThrowWhenExecutingQueriesIfCtasCreatesStream() {
    // Given:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "Invalid result type. Your SELECT query produces a STREAM. "
            + "Please use CREATE STREAM AS SELECT statement instead.")));
    expectedException.expect(statementText(is(
        "CREATE TABLE FOO AS SELECT * FROM ORDERS;")));

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "CREATE TABLE FOO AS SELECT * FROM ORDERS;",
        KAYPHER_CONFIG, Collections.emptyMap()
    );
  }

  @Test
  public void shouldThrowWhenTryExecuteCsasThatCreatesTable() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE STREAM FOO AS SELECT COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;").get(0));

    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "Invalid result type. Your SELECT query produces a TABLE. "
            + "Please use CREATE TABLE AS SELECT statement instead.")));
    expectedException.expect(statementText(is(
        "CREATE STREAM FOO AS SELECT COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;")));

    // When:
    sandbox.execute(
        serviceContext,
        ConfiguredStatement.of(statement, new HashMap<>(), KAYPHER_CONFIG)
    );
  }

  @Test
  public void shouldThrowWhenTryExecuteCtasThatCreatesStream() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE TABLE FOO AS SELECT * FROM ORDERS;").get(0));

    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(statementText(is("CREATE TABLE FOO AS SELECT * FROM ORDERS;")));
    expectedException.expect(rawMessage(is(
        "Invalid result type. Your SELECT query produces a STREAM. "
            + "Please use CREATE STREAM AS SELECT statement instead.")));

    // When:
    sandbox.execute(
        serviceContext,
        ConfiguredStatement.of(statement, new HashMap<>(), KAYPHER_CONFIG)
    );
  }

  @Test
  public void shouldThrowIfStatementMissingTopicConfig() {
    final List<ParsedStatement> parsed = parse(
        "CREATE TABLE FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='AVRO');"
            + "CREATE STREAM FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='AVRO');"
            + "CREATE TABLE FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='JSON');"
            + "CREATE STREAM FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='JSON');"
    );

    for (final ParsedStatement statement : parsed) {

      try {
        kaypherEngine.prepare(statement);
        Assert.fail();
      } catch (final KaypherStatementException e) {
        assertThat(e.getMessage(), containsString(
            "Missing required property \"KAFKA_TOPIC\" which has no default value."));
      }
    }
  }

  @Test
  public void shouldThrowIfStatementMissingValueFormatConfig() {
    // Given:
    givenTopicsExist("foo");

    final List<ParsedStatement> parsed = parse(
        "CREATE TABLE FOO (viewtime BIGINT, pageid VARCHAR) WITH (KAFKA_TOPIC='foo');"
            + "CREATE STREAM FOO (viewtime BIGINT, pageid VARCHAR) WITH (KAFKA_TOPIC='foo');"
    );

    for (final ParsedStatement statement : parsed) {

      try {
        // When:
        kaypherEngine.prepare(statement);

        // Then:
        Assert.fail();
      } catch (final KaypherStatementException e) {
        assertThat(e.getMessage(), containsString(
            "Missing required property \"VALUE_FORMAT\" which has no default value."));
      }
    }
  }

  @Test
  public void shouldThrowOnNoneExecutableDdlStatement() {
    // Given:
    expectedException.expect(KaypherStatementException.class);
    expectedException.expect(rawMessage(is("Statement not executable")));
    expectedException.expect(statementText(is("SHOW STREAMS;")));

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "SHOW STREAMS;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldNotUpdateMetaStoreDuringTryExecute() {
    // Given:
    final int numberOfLiveQueries = kaypherEngine.numberOfLiveQueries();
    final int numPersistentQueries = kaypherEngine.getPersistentQueries().size();

    final List<ParsedStatement> statements = parse(
            "CREATE STREAM S1 (COL1 BIGINT) WITH (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');"
            + "CREATE TABLE BAR AS SELECT * FROM TEST2;"
            + "CREATE TABLE FOO AS SELECT * FROM TEST2;"
            + "DROP TABLE TEST3;");

    topicClient.preconditionTopicExists("s1_topic", 1, (short) 1, Collections.emptyMap());

    // When:
    statements
        .forEach(stmt -> sandbox.execute(
            sandboxServiceContext,
            ConfiguredStatement.of(sandbox.prepare(stmt), new HashMap<>(), KAYPHER_CONFIG)));

    // Then:
    assertThat(metaStore.getSource(SourceName.of("TEST3")), is(notNullValue()));
    assertThat(metaStore.getQueriesWithSource(SourceName.of("TEST2")), is(empty()));
    assertThat(metaStore.getSource(SourceName.of("BAR")), is(nullValue()));
    assertThat(metaStore.getSource(SourceName.of("FOO")), is(nullValue()));
    assertThat("live", kaypherEngine.numberOfLiveQueries(), is(numberOfLiveQueries));
    assertThat("peristent", kaypherEngine.getPersistentQueries().size(), is(numPersistentQueries));
  }

  @Test
  public void shouldNotCreateAnyTopicsDuringTryExecute() {
    // Given:
    topicClient.preconditionTopicExists("s1_topic", 1, (short) 1, Collections.emptyMap());

    final List<ParsedStatement> statements = parse(
        "CREATE STREAM S1 (COL1 BIGINT) WITH (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');"
            + "CREATE TABLE BAR AS SELECT * FROM TEST2;"
            + "CREATE TABLE FOO AS SELECT * FROM TEST2;"
            + "DROP TABLE TEST3;");

    // When:
    statements.forEach(
        stmt -> sandbox.execute(
            sandboxServiceContext,
            ConfiguredStatement.of(sandbox.prepare(stmt), new HashMap<>(), KAYPHER_CONFIG))
    );

    // Then:
    assertThat("no topics should be created during a tryExecute call",
        topicClient.createdTopics().keySet(), is(empty()));
  }

  @Test
  public void shouldNotIncrementQueryIdCounterDuringTryExecute() {
    // Given:
    final String sql = "create table foo as select * from test2;";
    final PreparedStatement<?> statement = prepare(parse(sql).get(0));

    // When:
    sandbox.execute(
        sandboxServiceContext,
        ConfiguredStatement.of(statement, new HashMap<>(), KAYPHER_CONFIG)
    );

    // Then:
    final List<QueryMetadata> queries = KaypherEngineTestUtil
        .execute(serviceContext, kaypherEngine, sql, KAYPHER_CONFIG, Collections.emptyMap());
    assertThat("query id of actual execute should not be affected by previous tryExecute",
        ((PersistentQueryMetadata) queries.get(0)).getQueryId(), is(new QueryId("CTAS_FOO_0")));
  }

  @Test
  public void shouldNotRegisterAnySchemasDuringSandboxExecute() throws Exception {
    // Given:
    final List<ParsedStatement> statements = parse(
        "create table foo WITH(VALUE_FORMAT='AVRO') as select * from test2;"
            + "create stream foo2 WITH(VALUE_FORMAT='AVRO') as select * from orders;");

    givenStatementAlreadyExecuted(statements.get(0));

    final PreparedStatement<?> prepared = prepare(statements.get(1));

    // When:
    sandbox.execute(
        sandboxServiceContext,
        ConfiguredStatement.of(prepared, new HashMap<>(), KAYPHER_CONFIG)
    );

    // Then:
    verify(schemaRegistryClient, never()).register(any(), any());
  }

  @Test
  public void shouldOnlyUpdateSandboxOnQueryClose() {
    // Given:
    givenSqlAlreadyExecuted("create table bar as select * from test2;");

    final QueryId queryId = kaypherEngine.getPersistentQueries()
        .get(0).getQueryId();

    final PersistentQueryMetadata sandBoxQuery = sandbox.getPersistentQuery(queryId)
        .get();

    // When:
    sandBoxQuery.close();

    // Then:
    assertThat("main engine should not be updated",
        kaypherEngine.getPersistentQuery(queryId), is(not(Optional.empty())));

    assertThat("sand box should be updated",
        sandbox.getPersistentQuery(queryId), is(Optional.empty()));
  }

  @Test
  public void shouldRegisterPersistentQueriesOnlyInSandbox() {
    // Given:
    final PreparedStatement<?> prepared = prepare(parse(
        "create table bar as select * from test2;").get(0));

    // When:
    final ExecuteResult result = sandbox.execute(
        sandboxServiceContext,
        ConfiguredStatement.of(prepared, new HashMap<>(), KAYPHER_CONFIG)
    );

    // Then:
    assertThat(result.getQuery(), is(not(Optional.empty())));
    assertThat(sandbox.getPersistentQuery(getQueryId(result.getQuery().get())),
        is(not(Optional.empty())));
    assertThat(kaypherEngine.getPersistentQuery(getQueryId(result.getQuery().get())),
        is(Optional.empty()));
  }

  @Test
  public void shouldExecuteDdlStatement() {
    // Given:
    givenTopicsExist("foo");
    final PreparedStatement<?> statement =
        prepare(parse("CREATE STREAM FOO (a int) WITH (kafka_topic='foo', value_format='json');").get(0));

    // When:
    final ExecuteResult result = sandbox.execute(
        sandboxServiceContext,
        ConfiguredStatement.of(statement, new HashMap<>(), KAYPHER_CONFIG)
    );

    // Then:
    assertThat(result.getCommandResult(), is(Optional.of("Stream created")));
  }

  @Test
  public void shouldBeAbleToParseInvalidThings() {
    // Given:
    // No Stream called 'I_DO_NOT_EXIST' exists

    // When:
    final List<ParsedStatement> parsed = kaypherEngine
        .parse("CREATE STREAM FOO AS SELECT * FROM I_DO_NOT_EXIST;");

    // Then:
    assertThat(parsed, hasSize(1));
  }

  @Test
  public void shouldThrowOnPrepareIfSourcesDoNotExist() {
    // Given:
    final ParsedStatement parsed = kaypherEngine
        .parse("CREATE STREAM FOO AS SELECT * FROM I_DO_NOT_EXIST;")
        .get(0);

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("I_DO_NOT_EXIST does not exist");

    // When:
    kaypherEngine.prepare(parsed);
  }

  @Test
  public void shouldBeAbleToPrepareTerminateAndDrop() {
    // Given:
    givenSqlAlreadyExecuted("CREATE STREAM FOO AS SELECT * FROM TEST1;");

    final List<ParsedStatement> parsed = kaypherEngine.parse(
        "TERMINATE CSAS_FOO_0;"
            + "DROP STREAM FOO;");

    // When:
    parsed.forEach(kaypherEngine::prepare);

    // Then: did not throw.
  }

  @Test
  public void shouldIgnoreLegacyDeleteTopicPartOfDropCommand() {
    // Given:
    final QueryMetadata query = KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "CREATE STREAM FOO AS SELECT * FROM TEST1;",
        KAYPHER_CONFIG, Collections.emptyMap()
    ).get(0);
    query.close();

    // When:
    KaypherEngineTestUtil.execute(
        serviceContext,
        kaypherEngine,
        "DROP STREAM FOO DELETE TOPIC;",
        KAYPHER_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    verifyNoMoreInteractions(topicClient);
    verifyNoMoreInteractions(schemaRegistryClient);
  }

  private void givenTopicsExist(final String... topics) {
    givenTopicsExist(1, topics);
  }

  private void givenTopicsExist(final int partitionCount, final String... topics) {
    Arrays.stream(topics)
        .forEach(topic -> topicClient.createTopic(topic, partitionCount, (short) 1));
  }

  private List<ParsedStatement> parse(final String sql) {
    return kaypherEngine.parse(sql);
  }

  private PreparedStatement<?> prepare(final ParsedStatement stmt) {
    return kaypherEngine.prepare(stmt);
  }

  private void givenTopicWithSchema(final String topicName, final Schema schema) {
    try {
      givenTopicsExist(1, topicName);
      schemaRegistryClient.register(topicName + KaypherConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, schema);
    } catch (final Exception e) {
      fail("invalid test:" + e.getMessage());
    }
  }

  private static QueryId getQueryId(final QueryMetadata query) {
    return ((PersistentQueryMetadata) query).getQueryId();
  }

  private void givenStatementAlreadyExecuted(
      final ParsedStatement statement
  ) {
    kaypherEngine.execute(
        serviceContext,
        ConfiguredStatement.of(kaypherEngine.prepare(statement), new HashMap<>(), KAYPHER_CONFIG));
    sandbox = kaypherEngine.createSandbox(serviceContext);
  }

  private void givenSqlAlreadyExecuted(final String sql) {
    parse(sql).forEach(stmt ->
        kaypherEngine.execute(
            serviceContext,
            ConfiguredStatement.of(kaypherEngine.prepare(stmt), new HashMap<>(), KAYPHER_CONFIG)));

    sandbox = kaypherEngine.createSandbox(serviceContext);
  }
}