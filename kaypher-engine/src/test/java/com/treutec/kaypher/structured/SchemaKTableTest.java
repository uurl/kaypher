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
package com.treutec.kaypher.structured;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.same;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.ComparisonExpression;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.LongLiteral;
import com.treutec.kaypher.execution.plan.DefaultExecutionStepProperties;
import com.treutec.kaypher.execution.plan.ExecutionStep;
import com.treutec.kaypher.execution.plan.Formats;
import com.treutec.kaypher.execution.plan.JoinType;
import com.treutec.kaypher.execution.plan.KTableHolder;
import com.treutec.kaypher.execution.plan.KeySerdeFactory;
import com.treutec.kaypher.execution.plan.PlanBuilder;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.execution.plan.TableFilter;
import com.treutec.kaypher.execution.streams.AggregateParams;
import com.treutec.kaypher.execution.streams.ExecutionStepFactory;
import com.treutec.kaypher.execution.streams.GroupedFactory;
import com.treutec.kaypher.execution.streams.JoinedFactory;
import com.treutec.kaypher.execution.streams.KSPlanBuilder;
import com.treutec.kaypher.execution.streams.KaypherValueJoiner;
import com.treutec.kaypher.execution.streams.MaterializedFactory;
import com.treutec.kaypher.execution.streams.SqlPredicateFactory;
import com.treutec.kaypher.execution.streams.StreamJoinedFactory;
import com.treutec.kaypher.execution.streams.StreamsFactories;
import com.treutec.kaypher.execution.streams.StreamsUtil;
import com.treutec.kaypher.execution.util.StructKeyUtil;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.metastore.model.KaypherTable;
import com.treutec.kaypher.metastore.model.MetaStoreMatchers.KeyFieldMatchers;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.planner.plan.FilterNode;
import com.treutec.kaypher.planner.plan.PlanNode;
import com.treutec.kaypher.planner.plan.ProjectNode;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.PersistenceSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.GenericRowSerDe;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.structured.SchemaKStream.Type;
import com.treutec.kaypher.testutils.AnalysisTestUtil;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.MetaStoreFixture;
import com.treutec.kaypher.util.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKTableTest {

  private final KaypherConfig kaypherConfig = new KaypherConfig(Collections.emptyMap());
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final GroupedFactory groupedFactory = mock(GroupedFactory.class);
  private final Grouped grouped = Grouped.with(
      "group", Serdes.String(), Serdes.String());
  private final KeyField validKeyField = KeyField
      .of(Optional.of(ColumnRef.of(SourceName.of("left"), ColumnName.of("COL1"))));

  private SchemaKTable initialSchemaKTable;
  private KTable kTable;
  private KTable secondKTable;
  private KaypherTable<?> kaypherTable;
  private InternalFunctionRegistry functionRegistry;
  private KTable mockKTable;
  private SchemaKTable firstSchemaKTable;
  private SchemaKTable secondSchemaKTable;
  private LogicalSchema joinSchema;
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker().push("node");
  private final QueryContext.Stacker childContextStacker = queryContext.push("child");
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();
  private static final Expression TEST_2_COL_1 =
      new ColumnReferenceExp(ColumnRef.of(SourceName.of("TEST2"), ColumnName.of("COL1")));
  private static final Expression TEST_2_COL_2 =
      new ColumnReferenceExp(ColumnRef.of(SourceName.of("TEST2"), ColumnName.of("COL2")));
  private static final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(Format.JSON));
  private static final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(Format.JSON));

  private PlanBuilder planBuilder;

  @Mock
  private KaypherQueryBuilder queryBuilder;
  @Mock
  private KeySerdeFactory<Struct> keySerdeFactory;

  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    kaypherTable = (KaypherTable) metaStore.getSource(SourceName.of("TEST2"));
    final StreamsBuilder builder = new StreamsBuilder();
    kTable = builder.table(
        kaypherTable.getKaypherTopic().getKafkaTopicName(),
        Consumed.with(
            Serdes.String(),
            getRowSerde(kaypherTable.getKaypherTopic(), kaypherTable.getSchema().valueConnectSchema())
        ));

    final KaypherTable secondKaypherTable = (KaypherTable) metaStore.getSource(SourceName.of("TEST3"));
    secondKTable = builder.table(
        secondKaypherTable.getKaypherTopic().getKafkaTopicName(),
        Consumed.with(
            Serdes.String(),
            getRowSerde(secondKaypherTable.getKaypherTopic(), secondKaypherTable.getSchema().valueConnectSchema())
        ));

    mockKTable = EasyMock.niceMock(KTable.class);
    firstSchemaKTable = buildSchemaKTableForJoin(kaypherTable, mockKTable);
    secondSchemaKTable = buildSchemaKTableForJoin(secondKaypherTable, secondKTable);
    joinSchema = getJoinSchema(kaypherTable.getSchema(), secondKaypherTable.getSchema());

    when(queryBuilder.getQueryId()).thenReturn(new QueryId("query"));
    when(queryBuilder.getKaypherConfig()).thenReturn(kaypherConfig);
    when(queryBuilder.getProcessingLogContext()).thenReturn(processingLogContext);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    planBuilder = new KSPlanBuilder(
        queryBuilder,
        mock(SqlPredicateFactory.class),
        mock(AggregateParams.Factory.class),
        new StreamsFactories(
            groupedFactory,
            mock(JoinedFactory.class),
            mock(MaterializedFactory.class),
            mock(StreamJoinedFactory.class)
        )
    );
  }

  private ExecutionStep buildSourceStep(final LogicalSchema schema, final KTable kTable) {
    final ExecutionStep sourceStep = Mockito.mock(ExecutionStep.class);
    when(sourceStep.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(schema, queryContext.getQueryContext()));
    when(sourceStep.getSchema()).thenReturn(schema);
    when(sourceStep.build(any())).thenReturn(KTableHolder.unmaterialized(kTable, keySerdeFactory));
    return sourceStep;
  }

  private SchemaKTable buildSchemaKTable(
      final LogicalSchema schema,
      final KeyField keyField,
      final KTable kTable) {
    return new SchemaKTable(
        buildSourceStep(schema, kTable),
        keyFormat,
        keyField,
        new ArrayList<>(),
        Type.SOURCE,
        kaypherConfig,
        functionRegistry
    );
  }

  private SchemaKTable buildSchemaKTableFromPlan(final PlanNode logicalPlan) {
    return new SchemaKTable(
        buildSourceStep(logicalPlan.getTheSourceNode().getSchema(), kTable),
        keyFormat,
        logicalPlan.getTheSourceNode().getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        kaypherConfig,
        functionRegistry
    );
  }

  private SchemaKTable buildSchemaKTable(final KaypherTable kaypherTable, final KTable kTable) {
    final LogicalSchema schema = kaypherTable.getSchema().withAlias(kaypherTable.getName());

    final Optional<ColumnRef> newKeyName = kaypherTable.getKeyField().ref()
        .map(ref -> ref.withSource(kaypherTable.getName()));

    final KeyField keyFieldWithAlias = KeyField.of(newKeyName);

    return buildSchemaKTable(
        schema,
        keyFieldWithAlias,
        kTable
    );
  }

  private SchemaKTable buildSchemaKTableForJoin(final KaypherTable kaypherTable, final KTable kTable) {
    return buildSchemaKTable(
        kaypherTable.getSchema(), kaypherTable.getKeyField(), kTable
    );
  }

  private Serde<GenericRow> getRowSerde(final KaypherTopic topic, final ConnectSchema schema) {
    return GenericRowSerDe.from(
        topic.getValueFormat().getFormatInfo(),
        PersistenceSchema.from(schema, false),
        new KaypherConfig(Collections.emptyMap()),
        MockSchemaRegistryClient::new,
        "test",
        processingLogContext);
  }

  @Test
  public void testSelectSchemaKStream() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(
        projectNode.getSelectExpressions(),
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(projectedSchemaKStream.getSchema().value(), contains(
        Column.of(ColumnName.of("COL0"), SqlTypes.BIGINT),
        Column.of(ColumnName.of("COL2"), SqlTypes.STRING),
        Column.of(ColumnName.of("COL3"), SqlTypes.DOUBLE)
    ));

    assertThat(projectedSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKTable));
  }

  @Test
  public void shouldBuildStepForSelect() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(
        projectNode.getSelectExpressions(),
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(
        projectedSchemaKStream.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableMapValues(
                childContextStacker,
                initialSchemaKTable.getSourceTableStep(),
                projectNode.getSelectExpressions(),
                queryBuilder
            )
        )
    );
  }

  @Test
  public void testSelectWithExpression() {
    // Given:
    final String selectQuery = "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(
        projectNode.getSelectExpressions(),
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(projectedSchemaKStream.getSchema().value(), contains(
        Column.of(ColumnName.of("COL0"), SqlTypes.BIGINT),
        Column.of(ColumnName.of("KAYPHER_COL_1"), SqlTypes.INTEGER),
        Column.of(ColumnName.of("KAYPHER_COL_2"), SqlTypes.DOUBLE)
    ));

    assertThat(projectedSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKTable));
  }

  @Test
  public void shouldBuildSchemaKTableWithCorrectSchemaForFilter() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable filteredSchemaKStream = initialSchemaKTable.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    final SourceName test2 = SourceName.of("TEST2");
    assertThat(filteredSchemaKStream.getSchema().value(), contains(
        Column.of(test2, ColumnName.of("ROWTIME"), SqlTypes.BIGINT),
        Column.of(test2, ColumnName.of("ROWKEY"), SqlTypes.STRING),
        Column.of(test2, ColumnName.of("COL0"), SqlTypes.BIGINT),
        Column.of(test2, ColumnName.of("COL1"), SqlTypes.STRING),
        Column.of(test2, ColumnName.of("COL2"), SqlTypes.STRING),
        Column.of(test2, ColumnName.of("COL3"), SqlTypes.DOUBLE),
        Column.of(test2, ColumnName.of("COL4"), SqlTypes.BOOLEAN)
    ));

    assertThat(filteredSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKTable));
  }

  @Test
  public void shouldRewriteTimeComparisonInFilter() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 "
        + "WHERE ROWTIME = '1984-01-01T00:00:00+00:00' EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable filteredSchemaKTable = initialSchemaKTable.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    final TableFilter step = (TableFilter) filteredSchemaKTable.getSourceTableStep();
    assertThat(
        step.getFilterExpression(),
        Matchers.equalTo(
            new ComparisonExpression(
                ComparisonExpression.Type.EQUAL,
                new ColumnReferenceExp(ColumnRef.of(SourceName.of("TEST2"),
                    ColumnName.of("ROWTIME"))),
                new LongLiteral(441763200000L)
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForFilter() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable filteredSchemaKStream = initialSchemaKTable.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    assertThat(
        filteredSchemaKStream.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableFilter(
                childContextStacker,
                initialSchemaKTable.getSourceTableStep(),
                filterNode.getPredicate()
            )
        )
    );
  }

  @Test
  public void testGroupBy() {
    // Given:
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);
    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // When:
    final SchemaKGroupedTable groupedSchemaKTable = initialSchemaKTable.groupBy(
        valueFormat,
        groupByExpressions,
        childContextStacker
    );

    // Then:
    assertThat(groupedSchemaKTable, instanceOf(SchemaKGroupedTable.class));
    assertThat(groupedSchemaKTable.getKeyField().ref(), is(Optional.empty()));
  }

  @Test
  public void shouldBuildStepForGroupBy() {
    // Given:
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);
    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // When:
    final SchemaKGroupedTable groupedSchemaKTable = initialSchemaKTable.groupBy(
        valueFormat,
        groupByExpressions,
        childContextStacker
    );

    // Then:
    assertThat(
        groupedSchemaKTable.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableGroupBy(
                childContextStacker,
                initialSchemaKTable.getSourceTableStep(),
                Formats.of(initialSchemaKTable.keyFormat, valueFormat, SerdeOption.none()),
                groupByExpressions
            )
        )
    );
  }

  @Test
  public void shouldUseOpNameForGrouped() {
    // Given:
    final Serde<GenericRow> valSerde =
        getRowSerde(kaypherTable.getKaypherTopic(), kaypherTable.getSchema().valueConnectSchema());
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valSerde);
    expect(
        groupedFactory.create(
            eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
            anyObject(Serdes.String().getClass()),
            same(valSerde))
    ).andReturn(grouped);
    expect(mockKTable.filter(anyObject(Predicate.class))).andReturn(mockKTable);
    final KGroupedTable groupedTable = mock(KGroupedTable.class);
    expect(mockKTable.groupBy(anyObject(), same(grouped))).andReturn(groupedTable);
    replay(groupedFactory, mockKTable);

    final List<Expression> groupByExpressions = Collections.singletonList(TEST_2_COL_1);
    final SchemaKTable schemaKTable = buildSchemaKTable(kaypherTable, mockKTable);

    // When:
    final SchemaKGroupedTable result =
        schemaKTable.groupBy(valueFormat, groupByExpressions, childContextStacker);

    // Then:
    result.getSourceTableStep().build(planBuilder);
    verify(mockKTable, groupedFactory);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldGroupKeysCorrectly() {
    // set up a mock KTable and KGroupedTable for the test. Set up the KTable to
    // capture the mapper that is passed in to produce new keys
    final KTable mockKTable = mock(KTable.class);
    final KGroupedTable mockKGroupedTable = mock(KGroupedTable.class);
    final Capture<KeyValueMapper> capturedKeySelector = Capture.newInstance();
    expect(mockKTable.filter(anyObject(Predicate.class))).andReturn(mockKTable);
    expect(mockKTable.groupBy(capture(capturedKeySelector), anyObject(Grouped.class)))
        .andReturn(mockKGroupedTable);
    replay(mockKTable, mockKGroupedTable);

    // Build our test object from the mocks
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = new SchemaKTable(
        buildSourceStep(logicalPlan.getTheSourceNode().getSchema(), mockKTable),
        keyFormat,
        logicalPlan.getTheSourceNode().getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        kaypherConfig,
        functionRegistry
    );

    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // Call groupBy and extract the captured mapper
    final SchemaKGroupedTable result = initialSchemaKTable.groupBy(
        valueFormat, groupByExpressions, childContextStacker);
    result.getSourceTableStep().build(planBuilder);
    verify(mockKTable, mockKGroupedTable);
    final KeyValueMapper keySelector = capturedKeySelector.getValue();
    final GenericRow value = new GenericRow(Arrays.asList("key", 0, 100, "foo", "bar"));
    final KeyValue<String, GenericRow> keyValue =
        (KeyValue<String, GenericRow>) keySelector.apply("key", value);

    // Validate that the captured mapper produces the correct key
    assertThat(keyValue.key, equalTo(StructKeyUtil.asStructKey("bar|+|foo")));
    assertThat(keyValue.value, equalTo(value));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableLeftJoin() {
    expect(mockKTable.leftJoin(eq(secondKTable),
                               anyObject(KaypherValueJoiner.class)))
        .andReturn(EasyMock.niceMock(KTable.class));

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .leftJoin(
            secondSchemaKTable,
            joinSchema,
            validKeyField,
            childContextStacker);

    ((SchemaKTable) joinedKStream).getSourceTableStep().build(planBuilder);
    verify(mockKTable);
    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validKeyField));
    assertEquals(Arrays.asList(firstSchemaKTable, secondSchemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableInnerJoin() {
    expect(mockKTable.join(eq(secondKTable),
                           anyObject(KaypherValueJoiner.class)))
        .andReturn(EasyMock.niceMock(KTable.class));

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .join(secondSchemaKTable, joinSchema,
            validKeyField,
            childContextStacker);

    ((SchemaKTable) joinedKStream).getSourceTableStep().build(planBuilder);
    verify(mockKTable);
    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validKeyField));
    assertEquals(Arrays.asList(firstSchemaKTable, secondSchemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableOuterJoin() {
    expect(mockKTable.outerJoin(eq(secondKTable),
                                anyObject(KaypherValueJoiner.class)))
        .andReturn(EasyMock.niceMock(KTable.class));

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .outerJoin(secondSchemaKTable, joinSchema,
            validKeyField,
            childContextStacker);

    ((SchemaKTable) joinedKStream).getSourceTableStep().build(planBuilder);
    verify(mockKTable);
    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validKeyField));
    assertEquals(Arrays.asList(firstSchemaKTable, secondSchemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  interface Join {
    SchemaKTable join(
        SchemaKTable schemaKTable,
        LogicalSchema joinSchema,
        KeyField keyField,
        QueryContext.Stacker contextStacker
    );
  }

  @Test
  public void shouldBuildStepForTableTableJoin() {
    final KTable resultTable = EasyMock.niceMock(KTable.class);
    expect(mockKTable.outerJoin(
        eq(secondKTable),
        anyObject(KaypherValueJoiner.class))
    ).andReturn(resultTable);
    expect(mockKTable.join(
        eq(secondKTable),
        anyObject(KaypherValueJoiner.class))
    ).andReturn(resultTable);
    expect(mockKTable.leftJoin(
        eq(secondKTable),
        anyObject(KaypherValueJoiner.class))
    ).andReturn(resultTable);
    replay(mockKTable);

    final List<Pair<JoinType, Join>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, firstSchemaKTable::leftJoin),
        Pair.of(JoinType.INNER, firstSchemaKTable::join),
        Pair.of(JoinType.OUTER, firstSchemaKTable::outerJoin)
    );
    for (final Pair<JoinType, Join> testCase : cases) {
      final SchemaKTable result =
          testCase.right.join(secondSchemaKTable, joinSchema, validKeyField, childContextStacker);
      assertThat(
          result.getSourceTableStep(),
          equalTo(
              ExecutionStepFactory.tableTableJoin(
                  childContextStacker,
                  testCase.left,
                  firstSchemaKTable.getSourceTableStep(),
                  secondSchemaKTable.getSourceTableStep(),
                  joinSchema
              )
          )
      );
    }
  }

  @Test
  public void shouldUpdateKeyIfRenamed() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT col0 as NEWKEY, col2, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("NEWKEY")))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedViaFullyQualifiedName() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT test1.col0 as NEWKEY, col2, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("NEWKEY")))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedAndSourceIsAliased() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT t.col0 as NEWKEY, col2, col3 FROM test1 t EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("NEWKEY")))));
  }

  @Test
  public void shouldPreserveKeyOnSelectStar() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT * FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), KeyFieldMatchers.hasName("COL0"));
  }

  @Test
  public void shouldUpdateKeyIfMovedToDifferentIndex() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT col2, col0, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("COL0")))));
  }

  @Test
  public void shouldDropKeyIfNotSelected() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT col2, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void shouldHandleSourceWithoutKey() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT * FROM test4 EMIT CHANGES;");

    // When:
    final SchemaKTable result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void shouldSetKeyOnGroupBySingleExpressionThatIsInProjection() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT * FROM test2 EMIT CHANGES;");

    final SchemaKTable selected = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    final List<Expression> groupByExprs =  ImmutableList.of(TEST_2_COL_1);

    // When:
    final SchemaKGroupedTable result = selected
        .groupBy(valueFormat, groupByExprs, childContextStacker);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("COL1")))));
  }

  private static LogicalSchema getJoinSchema(
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final SourceName leftAlias = SourceName.of("left");
    final SourceName rightAlias = SourceName.of("right");
    for (final Column field : leftSchema.value()) {
      schemaBuilder.valueColumn(Column.of(leftAlias, field.name(), field.type()));
    }

    for (final Column field : rightSchema.value()) {
      schemaBuilder.valueColumn(Column.of(rightAlias, field.name(), field.type()));
    }
    return schemaBuilder.build();
  }

  private List<SelectExpression> givenInitialKTableOf(final String selectQuery) {
    final PlanNode logicalPlan = AnalysisTestUtil.buildLogicalPlan(
        kaypherConfig,
        selectQuery,
        metaStore
    );

    initialSchemaKTable = new SchemaKTable(
        buildSourceStep(logicalPlan.getTheSourceNode().getSchema(), kTable),
        keyFormat,
        logicalPlan.getTheSourceNode().getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        kaypherConfig,
        functionRegistry
    );

    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    return projectNode.getSelectExpressions();
  }

  private PlanNode buildLogicalPlan(final String query) {
    return AnalysisTestUtil.buildLogicalPlan(kaypherConfig, query, metaStore);
  }
}
