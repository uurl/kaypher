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
package com.treutec.kaypher.analyzer;

import static com.treutec.kaypher.testutils.AnalysisTestUtil.analyzeQuery;
import static com.treutec.kaypher.util.SchemaUtil.ROWTIME_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.treutec.kaypher.analyzer.Analysis.Into;
import com.treutec.kaypher.analyzer.Analysis.JoinInfo;
import com.treutec.kaypher.analyzer.Analyzer.SerdeOptionsSupplier;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.execution.expression.tree.BooleanLiteral;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.Literal;
import com.treutec.kaypher.execution.expression.tree.StringLiteral;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.metastore.model.KaypherStream;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.KaypherParserTestUtil;
import com.treutec.kaypher.parser.properties.with.CreateSourceAsProperties;
import com.treutec.kaypher.parser.tree.CreateStreamAsSelect;
import com.treutec.kaypher.parser.tree.Query;
import com.treutec.kaypher.parser.tree.Sink;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.planner.plan.JoinNode.JoinType;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.MetaStoreFixture;
import com.treutec.kaypher.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * DO NOT ADD NEW TESTS TO THIS FILE
 *
 * <p>Instead add new JSON based tests to QueryTranslationTest.
 *
 * <p>This test file is more of a functional test, which is better implemented using QTT.
 */
@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class AnalyzerFunctionalTest {

  private static final Set<SerdeOption> DEFAULT_SERDE_OPTIONS = SerdeOption.none();
  private static final SourceName TEST1 = SourceName.of("TEST1");
  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName COL1 = ColumnName.of("COL1");
  private static final ColumnName COL2 = ColumnName.of("COL2");
  private static final ColumnName COL3 = ColumnName.of("COL3");

  private MutableMetaStore jsonMetaStore;
  private MutableMetaStore avroMetaStore;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SerdeOptionsSupplier serdeOptiponsSupplier;
  @Mock
  private Sink sink;

  private Query query;
  private Analyzer analyzer;
  private Optional<Format> sinkFormat = Optional.empty();
  private Optional<Boolean> sinkWrapSingleValues = Optional.empty();

  @Before
  public void init() {
    jsonMetaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    avroMetaStore = MetaStoreFixture.getNewMetaStore(
        new InternalFunctionRegistry(),
        ValueFormat.of(FormatInfo.of(Format.AVRO))
    );

    analyzer = new Analyzer(
        jsonMetaStore,
        "",
        DEFAULT_SERDE_OPTIONS,
        serdeOptiponsSupplier
    );

    when(sink.getName()).thenReturn(SourceName.of("TEST0"));
    when(sink.getProperties()).thenReturn(CreateSourceAsProperties.none());

    query = parseSingle("Select COL0, COL1 from TEST1;");

    registerKafkaSource();
  }

  @Test
  public void testSimpleQueryAnalysis() {
    final String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;";
    final Analysis analysis = analyzeQuery(simpleQuery, jsonMetaStore);
    assertEquals("FROM was not analyzed correctly.",
        analysis.getFromDataSources().get(0).getDataSource().getName(),
        TEST1);
    assertThat(analysis.getWhereExpression().get().toString(), is("(TEST1.COL0 > 100)"));

    final List<SelectExpression> selects = analysis.getSelectExpressions();
    assertThat(selects.get(0).getExpression().toString(), is("TEST1.COL0"));
    assertThat(selects.get(1).getExpression().toString(), is("TEST1.COL2"));
    assertThat(selects.get(2).getExpression().toString(), is("TEST1.COL3"));

    assertThat(selects.get(0).getAlias(), is(COL0));
    assertThat(selects.get(1).getAlias(), is(COL2));
    assertThat(selects.get(2).getAlias(), is(COL3));
  }

  @Test
  public void testSimpleLeftJoinAnalysis() {
    // When:
    final Analysis analysis = analyzeQuery(
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 "
            + "FROM test1 t1 LEFT JOIN test2 t2 "
            + "ON t1.col1 = t2.col1 EMIT CHANGES;", jsonMetaStore);

    // Then:
    assertThat(analysis.getFromDataSources(), hasSize(2));
    assertThat(analysis.getFromDataSources().get(0).getAlias(), is(SourceName.of("T1")));
    assertThat(analysis.getFromDataSources().get(1).getAlias(), is(SourceName.of("T2")));

    assertThat(analysis.getJoin(), is(not(Optional.empty())));
    assertThat(analysis.getJoin().get().getLeftJoinField(), is(ColumnRef.of(SourceName.of("T1"),ColumnName.of("COL1"))));
    assertThat(analysis.getJoin().get().getRightJoinField(), is(ColumnRef.of(SourceName.of("T2"),ColumnName.of("COL1"))));

    final List<String> selects = analysis.getSelectExpressions().stream()
        .map(SelectExpression::getExpression)
        .map(Objects::toString)
        .collect(Collectors.toList());

    assertThat(selects, contains("T1.COL1", "T2.COL1", "T2.COL4", "T1.COL5", "T2.COL2"));

    final List<ColumnName> aliases = analysis.getSelectExpressions().stream()
        .map(SelectExpression::getAlias)
        .collect(Collectors.toList());

    assertThat(aliases.stream().map(ColumnName::name).collect(Collectors.toList()),
        contains("T1_COL1", "T2_COL1", "T2_COL4", "COL5", "T2_COL2"));
  }

  @Test
  public void shouldHandleJoinOnRowKey() {
    // When:
    final Optional<JoinInfo> join = analyzeQuery(
        "SELECT * FROM test1 t1 LEFT JOIN test2 t2 ON t1.ROWKEY = t2.ROWKEY EMIT CHANGES;",
        jsonMetaStore)
        .getJoin();

    // Then:
    assertThat(join, is(not(Optional.empty())));
    assertThat(join.get().getType(), is(JoinType.LEFT));
    assertThat(join.get().getLeftJoinField(), is(ColumnRef.of(SourceName.of("T1"),ColumnName.of("ROWKEY"))));
    assertThat(join.get().getRightJoinField(), is(ColumnRef.of(SourceName.of("T2"), ColumnName.of("ROWKEY"))));
  }

  @Test
  public void testBooleanExpressionAnalysis() {
    final String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1 EMIT CHANGES;";
    final Analysis analysis = analyzeQuery(queryStr, jsonMetaStore);

    assertEquals("FROM was not analyzed correctly.",
        analysis.getFromDataSources().get(0).getDataSource().getName(), TEST1);

    final List<SelectExpression> selects = analysis.getSelectExpressions();
    assertThat(selects.get(0).getExpression().toString(), is("(TEST1.COL0 = 10)"));
    assertThat(selects.get(1).getExpression().toString(), is("TEST1.COL2"));
    assertThat(selects.get(2).getExpression().toString(), is("(TEST1.COL3 > TEST1.COL1)"));
  }

  @Test
  public void testFilterAnalysis() {
    final String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1 WHERE col0 > 20 EMIT CHANGES;";
    final Analysis analysis = analyzeQuery(queryStr, jsonMetaStore);

    assertThat(analysis.getFromDataSources().get(0).getDataSource().getName(), is(TEST1));

    final List<SelectExpression> selects = analysis.getSelectExpressions();
    assertThat(selects.get(0).getExpression().toString(), is("(TEST1.COL0 = 10)"));
    assertThat(selects.get(1).getExpression().toString(), is("TEST1.COL2"));
    assertThat(selects.get(2).getExpression().toString(), is("(TEST1.COL3 > TEST1.COL1)"));
    assertThat(analysis.getWhereExpression().get().toString(), is("(TEST1.COL0 > 20)"));
  }

  @Test
  public void shouldCreateCorrectSinkKaypherTopic() {
    final String simpleQuery = "CREATE STREAM FOO WITH (KAFKA_TOPIC='TEST_TOPIC1') AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final List<Statement> statements = parse(simpleQuery, jsonMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));

    final Optional<Into> into = analysis.getInto();
    assertThat(into, is(not((Optional.empty()))));
    final KaypherTopic createdKaypherTopic = into.get().getKaypherTopic();
    assertThat(createdKaypherTopic.getKafkaTopicName(), is("TEST_TOPIC1"));
  }

  @Test
  public void shouldUseExplicitNamespaceForAvroSchema() {
    final String simpleQuery = "CREATE STREAM FOO WITH (VALUE_FORMAT='AVRO', VALUE_AVRO_SCHEMA_FULL_NAME='com.custom.schema', KAFKA_TOPIC='TEST_TOPIC1') AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final List<Statement> statements = parse(simpleQuery, jsonMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKaypherTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(Format.AVRO, Optional.of("com.custom.schema"),
            Optional.empty()))));
  }

  @Test
  public void shouldUseImplicitNamespaceForAvroSchema() {
    final String simpleQuery = "CREATE STREAM FOO WITH (VALUE_FORMAT='AVRO', KAFKA_TOPIC='TEST_TOPIC1') AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final List<Statement> statements = parse(simpleQuery, jsonMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKaypherTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(Format.AVRO))));
  }

    @Test
  public void shouldUseExplicitNamespaceWhenFormatIsInheritedForAvro() {
    final String simpleQuery = "create stream s1 with (VALUE_AVRO_SCHEMA_FULL_NAME='org.ac.s1') as select * from test1;";

    final List<Statement> statements = parse(simpleQuery, avroMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(avroMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
      assertThat(analysis.getInto().get().getKaypherTopic().getValueFormat(),
          is(ValueFormat.of(FormatInfo.of(Format.AVRO, Optional.of("org.ac.s1"), Optional.empty()))));
  }

  @Test
  public void shouldNotInheritNamespaceExplicitlySetUpstreamForAvro() {
    final String simpleQuery = "create stream s1 as select * from S0;";

    final MutableMetaStore newAvroMetaStore = avroMetaStore.copy();

    final KaypherTopic kaypherTopic = new KaypherTopic(
        "s0",
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.AVRO, Optional.of("org.ac.s1"), Optional.empty())),
        false);

    final LogicalSchema schema = LogicalSchema.builder()
            .valueColumn(ColumnName.of("FIELD1"), SqlTypes.BIGINT)
            .build();

    final KaypherStream<?> kaypherStream = new KaypherStream<>(
        "create stream s0 with(KAFKA_TOPIC='s0', VALUE_AVRO_SCHEMA_FULL_NAME='org.ac.s1', VALUE_FORMAT='avro');",
        SourceName.of("S0"),
        schema,
        SerdeOption.none(),
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("FIELD1"))),
        new MetadataTimestampExtractionPolicy(),
        kaypherTopic
    );

    newAvroMetaStore.putSource(kaypherStream);

    final List<Statement> statements = parse(simpleQuery, newAvroMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(newAvroMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKaypherTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(Format.AVRO))));
  }

  @Test
  public void shouldUseImplicitNamespaceWhenFormatIsInheritedForAvro() {
    final String simpleQuery = "create stream s1 as select * from test1;";

    final List<Statement> statements = parse(simpleQuery, avroMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(avroMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKaypherTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(Format.AVRO))));
  }

  @Test
  public void shouldFailIfExplicitNamespaceIsProvidedForNonAvroTopic() {
    final String simpleQuery = "CREATE STREAM FOO WITH (VALUE_FORMAT='JSON', VALUE_AVRO_SCHEMA_FULL_NAME='com.custom.schema', KAFKA_TOPIC='TEST_TOPIC1') AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final List<Statement> statements = parse(simpleQuery, jsonMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "", DEFAULT_SERDE_OPTIONS);

    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Full schema name only supported with AVRO format");

    analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));
  }

  @Test
  public void shouldFailIfExplicitNamespaceIsProvidedButEmpty() {
    final CreateStreamAsSelect createStreamAsSelect = parseSingle(
        "CREATE STREAM FOO "
            + "WITH (VALUE_FORMAT='AVRO', VALUE_AVRO_SCHEMA_FULL_NAME='', KAFKA_TOPIC='TEST_TOPIC1') "
            + "AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "", DEFAULT_SERDE_OPTIONS);

    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Schema name cannot be empty");

    analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));
  }

  @Test
  public void shouldGetSerdeOptions() {
    // Given:
    final Set<SerdeOption> serdeOptions = ImmutableSet.of(SerdeOption.UNWRAP_SINGLE_VALUES);
    when(serdeOptiponsSupplier.build(any(), any(), any(), any())).thenReturn(serdeOptions);

    givenSinkValueFormat(Format.AVRO);
    givenWrapSingleValues(true);

    // When:
    final Analysis result = analyzer.analyze(query, Optional.of(sink));

    // Then:
    verify(serdeOptiponsSupplier).build(
        ImmutableList.of("COL0", "COL1").stream().map(ColumnName::of).collect(Collectors.toList()),
        Format.AVRO,
        Optional.of(true),
        DEFAULT_SERDE_OPTIONS);

    assertThat(result.getSerdeOptions(), is(serdeOptions));
  }

  @Test
  public void shouldExcludeRowTimeAndRowKeyWhenGettingSerdeOptions() {
    // Given:
    final Set<SerdeOption> serdeOptions = ImmutableSet.of(SerdeOption.UNWRAP_SINGLE_VALUES);
    when(serdeOptiponsSupplier.build(any(), any(), any(), any())).thenReturn(serdeOptions);

    query = parseSingle("Select ROWTIME, ROWKEY, ROWTIME AS TIME, ROWKEY AS KEY, COL0, COL1 from TEST1;");

    // When:
    analyzer.analyze(query, Optional.of(sink));

    // Then:
    verify(serdeOptiponsSupplier).build(
        eq(ImmutableList.of("TIME", "KEY", "COL0", "COL1").stream().map(ColumnName::of).collect(Collectors.toList())),
        any(),
        any(),
        any());
  }

  @Test
  public void shouldThrowOnGroupByIfKafkaFormat() {
    // Given:
    query = parseSingle("Select COL0 from KAFKA_SOURCE GROUP BY COL0;");

    givenSinkValueFormat(Format.KAFKA);

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Source(s) KAFKA_SOURCE are using the 'KAFKA' value format."
        + " This format does not yet support GROUP BY.");

    // When:
    analyzer.analyze(query, Optional.of(sink));
  }

  @Test
  public void shouldThrowOnJoinIfKafkaFormat() {
    // Given:
    query = parseSingle("Select TEST1.COL0 from TEST1 JOIN KAFKA_SOURCE "
        + "WITHIN 1 SECOND ON "
        + "TEST1.COL0 = KAFKA_SOURCE.COL0;");

    givenSinkValueFormat(Format.KAFKA);

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Source(s) KAFKA_SOURCE are using the 'KAFKA' value format."
        + " This format does not yet support JOIN.");

    // When:
    analyzer.analyze(query, Optional.of(sink));
  }

  @Test
  public void shouldCaptureProjectionColumnRefs() {
    // Given:
    query = parseSingle("Select COL0, COL0 + COL1, SUBSTRING(COL2, 1) from TEST1;");

    // When:
    final Analysis analysis = analyzer.analyze(query, Optional.empty());

    // Then:
    assertThat(analysis.getSelectColumnRefs(), containsInAnyOrder(
        ColumnRef.of(TEST1, COL0),
        ColumnRef.of(TEST1, COL1),
        ColumnRef.of(TEST1, COL2)
    ));
  }

  @Test
  public void shouldIncludeMetaColumnsForSelectStarOnContinuousQueries() {
    // Given:
    query = parseSingle("Select * from TEST1 EMIT CHANGES;");

    // When:
    final Analysis analysis = analyzer.analyze(query, Optional.empty());

    // Then:
    assertThat(analysis.getSelectExpressions(), hasItem(
        SelectExpression.of(ROWTIME_NAME, new ColumnReferenceExp(ColumnRef.of(TEST1, ROWTIME_NAME)))
    ));
  }

  @Test
  public void shouldNotIncludeMetaColumnsForSelectStartOnStaticQueries() {
    // Given:
    query = parseSingle("Select * from TEST1;");

    // When:
    final Analysis analysis = analyzer.analyze(query, Optional.empty());

    // Then:
    assertThat(analysis.getSelectExpressions(), not(hasItem(
        SelectExpression.of(ROWTIME_NAME, new ColumnReferenceExp(ColumnRef.of(TEST1, ROWTIME_NAME)))
    )));
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> T parseSingle(final String simpleQuery) {
    return (T) Iterables.getOnlyElement(parse(simpleQuery, jsonMetaStore));
  }

  private void givenSinkValueFormat(final Format format) {
    this.sinkFormat = Optional.of(format);
    buildProps();
  }

  private void givenWrapSingleValues(final boolean wrap) {
    this.sinkWrapSingleValues = Optional.of(wrap);
    buildProps();
  }

  private void buildProps() {
    final Map<String, Literal> props = new HashMap<>();
    sinkFormat.ifPresent(f -> props.put("VALUE_FORMAT", new StringLiteral(f.toString())));
    sinkWrapSingleValues.ifPresent(b -> props.put("WRAP_SINGLE_VALUE", new BooleanLiteral(Boolean.toString(b))));

    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(props);

    when(sink.getProperties()).thenReturn(properties);

  }

  private void registerKafkaSource() {
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(COL0, SqlTypes.BIGINT)
        .build();

    final KaypherTopic topic = new KaypherTopic(
        "ks",
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.KAFKA)),
        false);

    final KaypherStream<?> stream = new KaypherStream<>(
        "sqlexpression",
        SourceName.of("KAFKA_SOURCE"),
        schema,
        SerdeOption.none(),
        KeyField.none(),
        new MetadataTimestampExtractionPolicy(),
        topic
    );

    jsonMetaStore.putSource(stream);
  }

  private static List<Statement> parse(final String simpleQuery, final MetaStore metaStore) {
    return KaypherParserTestUtil.buildAst(simpleQuery, metaStore)
        .stream()
        .map(PreparedStatement::getStatement)
        .collect(Collectors.toList());
  }
}
