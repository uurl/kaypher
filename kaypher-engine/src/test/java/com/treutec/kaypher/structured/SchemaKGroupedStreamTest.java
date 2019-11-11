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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.plan.ExecutionStep;
import com.treutec.kaypher.execution.plan.Formats;
import com.treutec.kaypher.execution.streams.ExecutionStepFactory;
import com.treutec.kaypher.execution.windows.KaypherWindowExpression;
import com.treutec.kaypher.execution.windows.SessionWindowExpression;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.model.WindowType;
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
import com.treutec.kaypher.serde.WindowInfo;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKGroupedStreamTest {
  private static final LogicalSchema AGG_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("AGG0"), SqlTypes.BIGINT)
      .build();
  private static final LogicalSchema OUT_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("IN0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("OUT0"), SqlTypes.STRING)
      .build();
  private static final FunctionCall AGG = new FunctionCall(
      FunctionName.of("SUM"),
      ImmutableList.of(new ColumnReferenceExp(ColumnRef.withoutSource(ColumnName.of("IN1"))))
  );
  private static final KaypherWindowExpression KAYPHER_WINDOW_EXP = new SessionWindowExpression(
      100, TimeUnit.SECONDS
  );

  @Mock
  private KeyField keyField;
  @Mock
  private List<SchemaKStream> sourceStreams;
  @Mock
  private KaypherConfig config;
  @Mock
  private FunctionCall aggCall;
  @Mock
  private WindowExpression windowExp;
  @Mock
  private ExecutionStep sourceStep;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private ValueFormat valueFormat;
  @Mock
  private KaypherQueryBuilder builder;

  private final FunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker().push("node");

  private SchemaKGroupedStream schemaGroupedStream;

  @Before
  public void setUp() {
    schemaGroupedStream = new SchemaKGroupedStream(
        sourceStep,
        keyFormat,
        keyField,
        sourceStreams,
        config,
        functionRegistry
    );
    when(windowExp.getKaypherWindowExpression()).thenReturn(KAYPHER_WINDOW_EXP);
    when(config.getBoolean(KaypherConfig.KAYPHER_WINDOWED_SESSION_KEY_LEGACY_CONFIG)).thenReturn(false);
  }

  @Test
  public void shouldReturnKTableWithOutputSchema() {
    // When:
    final SchemaKTable result = schemaGroupedStream.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(AGG),
        Optional.empty(),
        valueFormat,
        queryContext,
        builder
    );

    // Then:
    assertThat(result.getSchema(), is(OUT_SCHEMA));
  }

  @Test
  public void shouldBuildStepForAggregate() {
    // When:
    final SchemaKTable result = schemaGroupedStream.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(AGG),
        Optional.empty(),
        valueFormat,
        queryContext,
        builder
    );

    // Then:
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.streamAggregate(
                queryContext,
                schemaGroupedStream.getSourceStep(),
                OUT_SCHEMA,
                Formats.of(keyFormat, valueFormat, SerdeOption.none()),
                1,
                ImmutableList.of(AGG),
                AGG_SCHEMA
            )
        )
    );
  }

  @Test
  public void shouldBuildStepForWindowedAggregate() {
    // When:
    final SchemaKTable result = schemaGroupedStream.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        1,
        ImmutableList.of(AGG),
        Optional.of(windowExp),
        valueFormat,
        queryContext,
        builder
    );

    // Then:
    final KeyFormat expected = KeyFormat.windowed(
        FormatInfo.of(Format.KAFKA),
        WindowInfo.of(WindowType.SESSION, Optional.empty())
    );
    assertThat(
        result.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.streamWindowedAggregate(
                queryContext,
                schemaGroupedStream.getSourceStep(),
                OUT_SCHEMA,
                Formats.of(expected, valueFormat, SerdeOption.none()),
                1,
                ImmutableList.of(AGG),
                AGG_SCHEMA,
                KAYPHER_WINDOW_EXP
            )
        )
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnColumnCountMismatch() {
    // When:
    schemaGroupedStream.aggregate(
        AGG_SCHEMA,
        OUT_SCHEMA,
        2,
        ImmutableList.of(aggCall),
        Optional.of(windowExp),
        valueFormat,
        queryContext,
        builder
    );
  }
}
