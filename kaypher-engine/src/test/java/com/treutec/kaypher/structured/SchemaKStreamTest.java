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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.ComparisonExpression;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.expression.tree.LongLiteral;
import com.treutec.kaypher.execution.plan.DefaultExecutionStepProperties;
import com.treutec.kaypher.execution.plan.ExecutionStep;
import com.treutec.kaypher.execution.plan.ExecutionStepProperties;
import com.treutec.kaypher.execution.plan.Formats;
import com.treutec.kaypher.execution.plan.JoinType;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.execution.plan.StreamFilter;
import com.treutec.kaypher.execution.streams.ExecutionStepFactory;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.metastore.model.KaypherStream;
import com.treutec.kaypher.metastore.model.KaypherTable;
import com.treutec.kaypher.metastore.model.MetaStoreMatchers.KeyFieldMatchers;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.FunctionName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.planner.plan.FilterNode;
import com.treutec.kaypher.planner.plan.PlanNode;
import com.treutec.kaypher.planner.plan.ProjectNode;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.structured.SchemaKStream.Type;
import com.treutec.kaypher.testutils.AnalysisTestUtil;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.MetaStoreFixture;
import com.treutec.kaypher.util.Pair;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
@RunWith(MockitoJUnitRunner.class)
public class SchemaKStreamTest {

  private static final SourceName TEST1 = SourceName.of("TEST1");
  private static final Expression COL1 =
      new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL1")));

  private final KaypherConfig kaypherConfig = new KaypherConfig(Collections.emptyMap());
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final KeyField validJoinKeyField = KeyField
      .of(Optional.of(ColumnRef.of(SourceName.of("left"), ColumnName.of("COL1"))));
  private final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA));
  private final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(Format.JSON));
  private final ValueFormat rightFormat = ValueFormat.of(FormatInfo.of(Format.DELIMITED));
  private final LogicalSchema simpleSchema = LogicalSchema.builder()
      .valueColumn(ColumnName.of("key"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("val"), SqlTypes.BIGINT)
      .build();
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker().push("node");
  private final QueryContext.Stacker childContextStacker = queryContext.push("child");
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();

  private SchemaKStream initialSchemaKStream;
  private SchemaKStream secondSchemaKStream;
  private SchemaKTable schemaKTable;
  private KaypherStream<?> kaypherStream;
  private InternalFunctionRegistry functionRegistry;
  private LogicalSchema joinSchema;

  @Mock
  private ExecutionStepProperties tableSourceProperties;
  @Mock
  private ExecutionStep tableSourceStep;
  @Mock
  private ExecutionStepProperties sourceProperties;
  @Mock
  private ExecutionStep sourceStep;
  @Mock
  private KaypherQueryBuilder queryBuilder;

  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    kaypherStream = (KaypherStream) metaStore.getSource(SourceName.of("TEST1"));
    final KaypherStream secondKaypherStream = (KaypherStream) metaStore.getSource(SourceName.of("ORDERS"));
    secondSchemaKStream = buildSchemaKStreamForJoin(
        secondKaypherStream,
        mock(ExecutionStep.class)
    );
    final KaypherTable<?> kaypherTable = (KaypherTable) metaStore.getSource(SourceName.of("TEST2"));
    when(tableSourceStep.getProperties()).thenReturn(tableSourceProperties);
    when(tableSourceProperties.getSchema()).thenReturn(kaypherTable.getSchema());
    schemaKTable = new SchemaKTable(
        tableSourceStep,
        keyFormat,
        kaypherTable.getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        kaypherConfig,
        functionRegistry);
    when(sourceStep.getProperties()).thenReturn(sourceProperties);
    when(queryBuilder.getQueryId()).thenReturn(new QueryId("query"));
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(queryBuilder.getKaypherConfig()).thenReturn(kaypherConfig);
    when(queryBuilder.getProcessingLogContext()).thenReturn(processingLogContext);
    joinSchema = getJoinSchema(kaypherStream.getSchema(), secondKaypherStream.getSchema());
  }

  @Test
  public void testSelectSchemaKStream() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    final List<SelectExpression> selectExpressions = projectNode.getSelectExpressions();

    // When:
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(projectedSchemaKStream.getSchema().value(), contains(
        Column.of(ColumnName.of("COL0"), SqlTypes.BIGINT),
        Column.of(ColumnName.of("COL2"), SqlTypes.STRING),
        Column.of(ColumnName.of("COL3"), SqlTypes.DOUBLE)
    ));
    assertThat(projectedSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKStream));
  }

  @Test
  public void shouldBuildStepForSelect() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getSelectExpressions();

    // When:
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        selectExpressions,
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(
        projectedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamMapValues(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                selectExpressions,
                queryBuilder
            )
        )
    );
  }

  @Test
  public void shouldUpdateKeyIfRenamed() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0 as NEWKEY, col2, col3 FROM test1 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select( selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("NEWKEY")))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedViaFullyQualifiedName() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT test1.col0 as NEWKEY, col2, col3 FROM test1 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("NEWKEY")))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedAndSourceIsAliased() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT t.col0 as NEWKEY, col2, col3 FROM test1 t EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of(ColumnRef.withoutSource(ColumnName.of("NEWKEY")))));
  }

  @Test
  public void shouldPreserveKeyOnSelectStar() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT * FROM test1 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), KeyFieldMatchers.hasName("COL0"));
  }

  @Test
  public void shouldUpdateKeyIfMovedToDifferentIndex() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT col2, col0, col3 FROM test1 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        equalTo(KeyField.of(ColumnRef.withoutSource(ColumnName.of("COL0")))));
  }

  @Test
  public void shouldDropKeyIfNotSelected() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT col2, col3 FROM test1 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void shouldHandleSourceWithoutKey() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf("SELECT * FROM test4 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    final List<SelectExpression> selectExpressions = projectNode.getSelectExpressions();

    // When:
    final SchemaKStream result = initialSchemaKStream
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void testSelectWithExpression() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);

    // When:
    final SchemaKStream projectedSchemaKStream = initialSchemaKStream.select(
        projectNode.getSelectExpressions(),
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(projectedSchemaKStream.getSchema().value(), contains(
        Column.of(ColumnName.of("COL0"), SqlTypes.BIGINT),
        Column.of(ColumnName.of("KAYPHER_COL_1"), SqlTypes.INTEGER),
        Column.of(ColumnName.of("KAYPHER_COL_2"), SqlTypes.DOUBLE)
    ));

    assertThat(projectedSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKStream));
  }

  @Test
  public void shouldReturnSchemaKStreamWithCorrectSchemaForFilter() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    assertThat(filteredSchemaKStream.getSchema().value(), contains(
        Column.of(TEST1, ColumnName.of("ROWTIME"), SqlTypes.BIGINT),
        Column.of(TEST1, ColumnName.of("ROWKEY"), SqlTypes.STRING),
        Column.of(TEST1, ColumnName.of("COL0"), SqlTypes.BIGINT),
        Column.of(TEST1, ColumnName.of("COL1"), SqlTypes.STRING),
        Column.of(TEST1, ColumnName.of("COL2"), SqlTypes.STRING),
        Column.of(TEST1, ColumnName.of("COL3"), SqlTypes.DOUBLE),
        Column.of(TEST1, ColumnName.of("COL4"), SqlTypes.array(SqlTypes.DOUBLE)),
        Column.of(TEST1, ColumnName.of("COL5"), SqlTypes.map(SqlTypes.DOUBLE))
    ));

    assertThat(filteredSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKStream));
  }

  @Test
  public void shouldRewriteTimeComparisonInFilter() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 "
            + "WHERE ROWTIME = '1984-01-01T00:00:00+00:00' EMIT CHANGES;");
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    final StreamFilter step = (StreamFilter) filteredSchemaKStream.getSourceStep();
    assertThat(
        step.getFilterExpression(),
        equalTo(
            new ComparisonExpression(
                ComparisonExpression.Type.EQUAL,
                new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("ROWTIME"))),
                new LongLiteral(441763200000L)
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForFilter() {
    // Given:
    final PlanNode logicalPlan = givenInitialKStreamOf(
        "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);

    // When:
    final SchemaKStream filteredSchemaKStream = initialSchemaKStream.filter(
        filterNode.getPredicate(),
        childContextStacker
    );

    // Then:
    assertThat(
        filteredSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamFilter(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                filterNode.getPredicate()
            )
        )
    );
  }

  @Test
  public void shouldSelectKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final KeyField expected = KeyField
        .of(ColumnRef.of(SourceName.of("TEST1"), ColumnName.of("COL1")));

    // When:
    final SchemaKStream<?> rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        ColumnRef.of(SourceName.of("TEST1"), ColumnName.of("COL1")),
        true,
        childContextStacker);

    // Then:
    assertThat(rekeyedSchemaKStream.getKeyField(), is(expected));
  }

  @Test
  public void shouldBuildStepForSelectKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    // When:
    final SchemaKStream<?> rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        ColumnRef.of(SourceName.of("TEST1"), ColumnName.of("COL1")),
        true,
        childContextStacker);

    // Then:
    assertThat(
        rekeyedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamSelectKey(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                ColumnRef.of(SourceName.of("TEST1"), ColumnName.of("COL1")),
                true
            )
        )
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnSelectKeyIfKeyNotInSchema() {
    givenInitialKStreamOf("SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final SchemaKStream<?> rekeyedSchemaKStream = initialSchemaKStream.selectKey(
        ColumnRef.withoutSource(ColumnName.of("won't find me")),
        true,
        childContextStacker);

    assertThat(rekeyedSchemaKStream.getKeyField(), is(validJoinKeyField));
  }

  @Test
  public void testGroupByKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final List<Expression> groupBy = Collections.singletonList(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL0")))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat,
        groupBy,
        childContextStacker
    );

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().ref(), is(Optional.of(ColumnRef.of(SourceName.of("TEST1"), ColumnName.of("COL0")))));
  }

  @Test
  public void shouldBuildStepForGroupByKey() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final List<Expression> groupBy = Collections.singletonList(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL0")))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat,
        groupBy,
        childContextStacker
    );

    // Then:
    final KeyFormat expectedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());
    assertThat(
        groupedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamGroupByKey(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                Formats.of(expectedKeyFormat, valueFormat, SerdeOption.none())
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForGroupBy() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");
    final List<Expression> groupBy = Collections.singletonList(
            new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL1")))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat,
        groupBy,
        childContextStacker
    );

    // Then:
    final KeyFormat expectedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());
    assertThat(
        groupedSchemaKStream.getSourceStep(),
        equalTo(
            ExecutionStepFactory.streamGroupBy(
                childContextStacker,
                initialSchemaKStream.getSourceStep(),
                Formats.of(expectedKeyFormat, valueFormat, SerdeOption.none()),
                groupBy
            )
        )
    );
  }

  @Test
  public void testGroupByMultipleColumns() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final List<Expression> groupBy = ImmutableList.of(
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL1"))),
        new ColumnReferenceExp(ColumnRef.of(TEST1, ColumnName.of("COL0")))
    );

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat,
        groupBy,
        childContextStacker
    );

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().ref(), is(Optional.empty()));
  }

  @Test
  public void testGroupByMoreComplexExpression() {
    // Given:
    givenInitialKStreamOf("SELECT col0, col1 FROM test1 WHERE col0 > 100 EMIT CHANGES;");

    final Expression groupBy = new FunctionCall(FunctionName.of("UCASE"), ImmutableList.of(COL1));

    // When:
    final SchemaKGroupedStream groupedSchemaKStream = initialSchemaKStream.groupBy(
        valueFormat,
        ImmutableList.of(groupBy),
        childContextStacker
    );

    // Then:
    assertThat(groupedSchemaKStream.getKeyField().ref(), is(Optional.empty()));
  }

  @Test
  public void shouldBuildStepForToTable() {
    // Given:
    givenInitialSchemaKStreamUsesMocks();

    // When:
    final SchemaKTable result = initialSchemaKStream.toTable(
        keyFormat,
        valueFormat,
        childContextStacker
    );

    // Then:
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.streamToTable(
                childContextStacker,
                Formats.of(keyFormat, valueFormat, SerdeOption.none()),
                sourceStep
            )
        )
    );
  }

  @Test
  public void shouldConvertToTableWithCorrectProperties() {
    // Given:
    givenInitialSchemaKStreamUsesMocks();

    // When:
    final SchemaKTable result = initialSchemaKStream.toTable(
        keyFormat,
        valueFormat,
        childContextStacker
    );

    // Then:
    assertThat(result.getSchema(), is(initialSchemaKStream.getSchema()));
    assertThat(result.getKeyField(), is(initialSchemaKStream.getKeyField()));
  }

  @FunctionalInterface
  private interface StreamStreamJoin {
    SchemaKStream join(
        SchemaKStream otherSchemaKStream,
        LogicalSchema joinSchema,
        KeyField keyField,
        JoinWindows joinWindows,
        ValueFormat leftFormat,
        ValueFormat rightFormat,
        QueryContext.Stacker contextStacker
    );
  }

  @Test
  public void shouldBuildStepForStreamStreamJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(kaypherStream);
    final JoinWindows joinWindow = JoinWindows.of(Duration.ofMillis(10L));

    final List<Pair<JoinType, StreamStreamJoin>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, initialSchemaKStream::leftJoin),
        Pair.of(JoinType.INNER, initialSchemaKStream::join),
        Pair.of(JoinType.OUTER, initialSchemaKStream::outerJoin)
    );

    for (final Pair<JoinType, StreamStreamJoin> testcase : cases) {
      final SchemaKStream joinedKStream = testcase.right.join(
          secondSchemaKStream,
          joinSchema,
          validJoinKeyField,
          joinWindow,
          valueFormat,
          rightFormat,
          childContextStacker
      );

      // Then:
      assertThat(
          joinedKStream.getSourceStep(),
          equalTo(
              ExecutionStepFactory.streamStreamJoin(
                  childContextStacker,
                  testcase.left,
                  Formats.of(keyFormat, valueFormat, SerdeOption.none()),
                  Formats.of(keyFormat, rightFormat, SerdeOption.none()),
                  initialSchemaKStream.getSourceStep(),
                  secondSchemaKStream.getSourceStep(),
                  joinSchema,
                  joinWindow
              )
          )
      );
    }
  }

  @FunctionalInterface
  private interface StreamTableJoin {
    SchemaKStream join(
        SchemaKTable other,
        LogicalSchema joinSchema,
        KeyField keyField,
        ValueFormat leftFormat,
        QueryContext.Stacker contextStacker
    );
  }

  @Test
  public void shouldBuildStepForStreamTableJoin() {
    // Given:
    final SchemaKStream initialSchemaKStream = buildSchemaKStreamForJoin(kaypherStream);

    final List<Pair<JoinType, StreamTableJoin>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, initialSchemaKStream::leftJoin),
        Pair.of(JoinType.INNER, initialSchemaKStream::join)
    );

    for (final Pair<JoinType, StreamTableJoin> testcase : cases) {
      final SchemaKStream joinedKStream = testcase.right.join(
          schemaKTable,
          joinSchema,
          validJoinKeyField,
          valueFormat,
          childContextStacker
      );

      // Then:
      assertThat(
          joinedKStream.getSourceStep(),
          equalTo(
              ExecutionStepFactory.streamTableJoin(
                  childContextStacker,
                  testcase.left,
                  Formats.of(keyFormat, valueFormat, SerdeOption.none()),
                  initialSchemaKStream.getSourceStep(),
                  schemaKTable.getSourceTableStep(),
                  joinSchema
              )
          )
      );
    }
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectly() {
    // Given:
    when(sourceProperties.getSchema()).thenReturn(simpleSchema);
    final SchemaKStream parentSchemaKStream = mock(SchemaKStream.class);
    when(parentSchemaKStream.getExecutionPlan(any(), anyString()))
        .thenReturn("parent plan");
    when(sourceProperties.getQueryContext()).thenReturn(
        queryContext.push("source").getQueryContext());
    final SchemaKStream schemaKtream = new SchemaKStream(
        sourceStep,
        keyFormat,
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("key"))),
        ImmutableList.of(parentSchemaKStream),
        Type.SOURCE,
        kaypherConfig,
        functionRegistry
    );

    // When/Then:
    assertThat(schemaKtream.getExecutionPlan(new QueryId("query"), ""), equalTo(
        " > [ SOURCE ] | Schema: [ROWKEY STRING KEY, `key` STRING, `val` BIGINT] | "
            + "Logger: query.node.source\n"
            + "\tparent plan"));
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectlyForRoot() {
    // Given:
    when(sourceProperties.getSchema()).thenReturn(simpleSchema);
    when(sourceProperties.getQueryContext()).thenReturn(
        queryContext.push("source").getQueryContext());
    final SchemaKStream schemaKtream = new SchemaKStream(
        sourceStep,
        keyFormat,
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("key"))),
        Collections.emptyList(),
        Type.SOURCE,
        kaypherConfig,
        functionRegistry
    );

    // When/Then:
    assertThat(schemaKtream.getExecutionPlan(new QueryId("query"), ""), equalTo(
        " > [ SOURCE ] | Schema: [ROWKEY STRING KEY, `key` STRING, `val` BIGINT] | "
            + "Logger: query.node.source\n"));
  }

  @Test
  public void shouldSummarizeExecutionPlanCorrectlyWhenMultipleParents() {
    // Given:
    final SchemaKStream parentSchemaKStream1 = mock(SchemaKStream.class);
    when(parentSchemaKStream1.getExecutionPlan(any(), anyString()))
        .thenReturn("parent 1 plan");
    final SchemaKStream parentSchemaKStream2 = mock(SchemaKStream.class);
    when(parentSchemaKStream2.getExecutionPlan(any(), anyString()))
        .thenReturn("parent 2 plan");
    when(sourceProperties.getSchema()).thenReturn(simpleSchema);
    when(sourceProperties.getQueryContext()).thenReturn(
        queryContext.push("source").getQueryContext());
    final SchemaKStream schemaKtream = new SchemaKStream(
        sourceStep,
        keyFormat,
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("key"))),
        ImmutableList.of(parentSchemaKStream1, parentSchemaKStream2),
        Type.SOURCE,
        kaypherConfig,
        functionRegistry
    );

    // When/Then:
    assertThat(schemaKtream.getExecutionPlan(new QueryId("query"), ""), equalTo(
        " > [ SOURCE ] | Schema: [ROWKEY STRING KEY, `key` STRING, `val` BIGINT] | "
            + "Logger: query.node.source\n"
            + "\tparent 1 plan"
            + "\tparent 2 plan"));
  }

  private void givenSourcePropertiesWithSchema(final LogicalSchema schema) {
    reset(sourceProperties);
    when(sourceProperties.getSchema()).thenReturn(schema);
    when(sourceProperties.withQueryContext(any())).thenAnswer(
        i -> new DefaultExecutionStepProperties(schema, (QueryContext) i.getArguments()[0])
    );
  }

  private SchemaKStream buildSchemaKStream(
      final LogicalSchema schema,
      final KeyField keyField,
      final ExecutionStep sourceStep) {
    givenSourcePropertiesWithSchema(schema);
    return new SchemaKStream(
        sourceStep,
        sourceProperties,
        keyFormat,
        keyField,
        new ArrayList<>(),
        Type.SOURCE,
        kaypherConfig,
        functionRegistry
    );
  }

  private void givenInitialSchemaKStreamUsesMocks() {
    final LogicalSchema schema = kaypherStream.getSchema().withAlias(kaypherStream.getName());

    final Optional<ColumnRef> newKeyName = kaypherStream.getKeyField()
        .ref()
        .map(ref -> ref.withSource(kaypherStream.getName()));

    initialSchemaKStream = buildSchemaKStream(
        schema,
        KeyField.of(newKeyName),
        sourceStep
    );
  }

  private SchemaKStream buildSchemaKStreamForJoin(final KaypherStream kaypherStream) {
    return buildSchemaKStream(
        kaypherStream.getSchema().withAlias(SourceName.of("test")),
        kaypherStream.getKeyField().withAlias(SourceName.of("test")),
        sourceStep
    );
  }

  private SchemaKStream buildSchemaKStreamForJoin(
      final KaypherStream kaypherStream,
      final ExecutionStep sourceStep) {
    return buildSchemaKStream(
        kaypherStream.getSchema().withAlias(SourceName.of("test")),
        kaypherStream.getKeyField().withAlias(SourceName.of("test")),
        sourceStep
    );
  }

  private static LogicalSchema getJoinSchema(
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final String leftAlias = "left";
    final String rightAlias = "right";
    for (final Column field : leftSchema.value()) {
      schemaBuilder.valueColumn(Column.of(SourceName.of(leftAlias), field.name(), field.type()));
    }

    for (final Column field : rightSchema.value()) {
      schemaBuilder.valueColumn(Column.of(SourceName.of(rightAlias), field.name(), field.type()));
    }
    return schemaBuilder.build();
  }

  private PlanNode givenInitialKStreamOf(final String selectQuery) {
    final PlanNode logicalPlan = AnalysisTestUtil.buildLogicalPlan(
        kaypherConfig,
        selectQuery,
        metaStore
    );
    givenSourcePropertiesWithSchema(logicalPlan.getTheSourceNode().getSchema());
    initialSchemaKStream = new SchemaKStream(
        sourceStep,
        keyFormat,
        logicalPlan.getTheSourceNode().getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        kaypherConfig,
        functionRegistry
    );

    return logicalPlan;
  }
}
