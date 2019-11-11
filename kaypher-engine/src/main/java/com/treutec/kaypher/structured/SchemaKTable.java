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

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.plan.ExecutionStep;
import com.treutec.kaypher.execution.plan.Formats;
import com.treutec.kaypher.execution.plan.JoinType;
import com.treutec.kaypher.execution.plan.KTableHolder;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.execution.plan.TableFilter;
import com.treutec.kaypher.execution.plan.TableGroupBy;
import com.treutec.kaypher.execution.plan.TableMapValues;
import com.treutec.kaypher.execution.plan.TableSink;
import com.treutec.kaypher.execution.plan.TableTableJoin;
import com.treutec.kaypher.execution.streams.ExecutionStepFactory;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class SchemaKTable<K> extends SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final ExecutionStep<KTableHolder<K>> sourceTableStep;

  public SchemaKTable(
      final ExecutionStep<KTableHolder<K>> sourceTableStep,
      final KeyFormat keyFormat,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Type type,
      final KaypherConfig kaypherConfig,
      final FunctionRegistry functionRegistry
  ) {
    super(
        null,
        Objects.requireNonNull(sourceTableStep, "sourceTableStep").getProperties(),
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        type,
        kaypherConfig,
        functionRegistry
    );
    this.sourceTableStep = sourceTableStep;
  }

  @Override
  public SchemaKTable<K> into(
      final String kafkaTopicName,
      final LogicalSchema outputSchema,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options,
      final QueryContext.Stacker contextStacker
  ) {
    final TableSink<K> step = ExecutionStepFactory.tableSink(
        contextStacker,
        outputSchema,
        sourceTableStep,
        Formats.of(keyFormat, valueFormat, options),
        kafkaTopicName
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        type,
        kaypherConfig,
        functionRegistry
    );
  }

  @Override
  public SchemaKTable<K> filter(
      final Expression filterExpression,
      final QueryContext.Stacker contextStacker
  ) {
    final TableFilter<K> step = ExecutionStepFactory.tableFilter(
        contextStacker,
        sourceTableStep,
        rewriteTimeComparisonForFilter(filterExpression)
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        Collections.singletonList(this),
        Type.FILTER,
        kaypherConfig,
        functionRegistry
    );
  }

  @Override
  public SchemaKTable<K> select(
      final List<SelectExpression> selectExpressions,
      final QueryContext.Stacker contextStacker,
      final KaypherQueryBuilder kaypherQueryBuilder) {
    final KeySelection selection = new KeySelection(
        selectExpressions
    );
    final TableMapValues<K> step = ExecutionStepFactory.tableMapValues(
        contextStacker,
        sourceTableStep,
        selectExpressions,
        kaypherQueryBuilder
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        selection.getKey(),
        Collections.singletonList(this),
        Type.PROJECT,
        kaypherConfig,
        functionRegistry
    );
  }

  @Override
  public ExecutionStep<?> getSourceStep() {
    return sourceTableStep;
  }

  public ExecutionStep<KTableHolder<K>> getSourceTableStep() {
    return sourceTableStep;
  }

  @Override
  public SchemaKGroupedTable groupBy(
      final ValueFormat valueFormat,
      final List<Expression> groupByExpressions,
      final QueryContext.Stacker contextStacker
  ) {

    final KeyFormat groupedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());

    final ColumnRef aggregateKeyName = groupedKeyNameFor(groupByExpressions);
    final Optional<ColumnRef> newKeyField = getSchema()
        .findValueColumn(aggregateKeyName.withoutSource())
        .map(Column::ref);

    final TableGroupBy<K> step = ExecutionStepFactory.tableGroupBy(
        contextStacker,
        sourceTableStep,
        Formats.of(groupedKeyFormat, valueFormat, SerdeOption.none()),
        groupByExpressions
    );
    return new SchemaKGroupedTable(
        step,
        groupedKeyFormat,
        KeyField.of(newKeyField),
        Collections.singletonList(this),
        kaypherConfig,
        functionRegistry);
  }

  public SchemaKTable<K> join(
      final SchemaKTable<K> schemaKTable,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final QueryContext.Stacker contextStacker
  ) {
    final TableTableJoin<K> step = ExecutionStepFactory.tableTableJoin(
        contextStacker,
        JoinType.INNER,
        sourceTableStep,
        schemaKTable.getSourceTableStep(),
        joinSchema
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        kaypherConfig,
        functionRegistry
    );
  }

  public SchemaKTable<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final QueryContext.Stacker contextStacker
  ) {
    final TableTableJoin<K> step = ExecutionStepFactory.tableTableJoin(
        contextStacker,
        JoinType.LEFT,
        sourceTableStep,
        schemaKTable.getSourceTableStep(),
        joinSchema
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        kaypherConfig,
        functionRegistry
    );
  }

  public SchemaKTable<K> outerJoin(
      final SchemaKTable<K> schemaKTable,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final QueryContext.Stacker contextStacker
  ) {
    final TableTableJoin<K> step = ExecutionStepFactory.tableTableJoin(
        contextStacker,
        JoinType.OUTER,
        sourceTableStep,
        schemaKTable.getSourceTableStep(),
        joinSchema
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        kaypherConfig,
        functionRegistry
    );
  }
}
