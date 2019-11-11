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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.plan.ExecutionStep;
import com.treutec.kaypher.execution.plan.Formats;
import com.treutec.kaypher.execution.streams.ExecutionStepFactory;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.FunctionName;
import com.treutec.kaypher.parser.tree.WindowExpression;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import java.util.Collections;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKGroupedTableTest {
  private static final LogicalSchema IN_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("IN1"), SqlTypes.INTEGER)
      .build();
  private static final LogicalSchema AGG_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("AGG0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("AGG1"), SqlTypes.BIGINT)
      .build();
  private static final LogicalSchema OUT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("OUT0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("OUT1"), SqlTypes.STRING)
      .build();
  private static final FunctionCall MIN = udaf("MIN");
  private static final FunctionCall MAX = udaf("MAX");
  private static final FunctionCall SUM = udaf("SUM");
  private static final FunctionCall COUNT = udaf("COUNT");

  private final KaypherConfig kaypherConfig = new KaypherConfig(Collections.emptyMap());
  private final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker().push("node");
  private final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(Format.JSON));
  private final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(Format.JSON));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KaypherQueryBuilder queryBuilder;

  @Before
  public void init() {
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
  }

  private static <S> ExecutionStep<S> buildSourceTableStep(final LogicalSchema schema) {
    final ExecutionStep<S> step = mock(ExecutionStep.class);
    when(step.getSchema()).thenReturn(schema);
    return step;
  }

  @Test
  public void shouldFailWindowedTableAggregation() {
    // Given:
    final WindowExpression windowExp = mock(WindowExpression.class);

    final SchemaKGroupedTable groupedTable = buildSchemaKGroupedTable();

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Windowing not supported for table aggregations.");

    // When:
    groupedTable.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(SUM, COUNT),
        Optional.of(windowExp),
        valueFormat,
        queryContext,
        queryBuilder
    );
  }

  @Test
  public void shouldFailUnsupportedAggregateFunction() {
    // Given:
    final SchemaKGroupedTable kGroupedTable = buildSchemaKGroupedTable();

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(
        "The aggregation function(s) (MIN, MAX) cannot be applied to a table.");

    // When:
    kGroupedTable.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(MIN, MAX),
        Optional.empty(),
        valueFormat,
        queryContext,
        queryBuilder
    );
  }

  private SchemaKGroupedTable buildSchemaKGroupedTable() {
    return new SchemaKGroupedTable(
        buildSourceTableStep(IN_SCHEMA),
        keyFormat,
        KeyField.of(IN_SCHEMA.value().get(0).ref()),
        Collections.emptyList(),
        kaypherConfig,
        functionRegistry
    );
  }

  @Test
  public void shouldBuildStepForAggregate() {
    // Given:
    final SchemaKGroupedTable kGroupedTable = buildSchemaKGroupedTable();

    final SchemaKTable result = kGroupedTable.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(SUM, COUNT),
        Optional.empty(),
        valueFormat,
        queryContext,
        queryBuilder
    );

    // Then:
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableAggregate(
                queryContext,
                kGroupedTable.getSourceTableStep(),
                OUT_SCHEMA,
                Formats.of(keyFormat, valueFormat, SerdeOption.none()),
                1,
                ImmutableList.of(SUM, COUNT),
                AGG_SCHEMA
            )
        )
    );
  }

  @Test
  public void shouldReturnKTableWithOutputSchema() {
    // Given:
    final SchemaKGroupedTable groupedTable = buildSchemaKGroupedTable();

    // When:
    final SchemaKTable result = groupedTable.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(SUM, COUNT),
        Optional.empty(),
        valueFormat,
        queryContext,
        queryBuilder
    );

    // Then:
    assertThat(result.getSchema(), is(OUT_SCHEMA));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnColumnCountMismatch() {
    // Given:
    final SchemaKGroupedTable groupedTable = buildSchemaKGroupedTable();

    // When:
    groupedTable.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        2,
        ImmutableList.of(SUM, COUNT),
        Optional.empty(),
        valueFormat,
        queryContext,
        queryBuilder
    );
  }

  private static FunctionCall udaf(final String name) {
    return new FunctionCall(
        FunctionName.of(name),
        ImmutableList.of(new ColumnReferenceExp(ColumnRef.withoutSource(ColumnName.of("IN1"))))
    );
  }
}
