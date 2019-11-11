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

import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.function.TableAggregationFunction;
import com.treutec.kaypher.execution.function.UdafUtil;
import com.treutec.kaypher.execution.plan.ExecutionStep;
import com.treutec.kaypher.execution.plan.Formats;
import com.treutec.kaypher.execution.plan.TableAggregate;
import com.treutec.kaypher.execution.streams.ExecutionStepFactory;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.function.KaypherAggregateFunction;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.FunctionName;
import com.treutec.kaypher.parser.tree.WindowExpression;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedTable;

public class SchemaKGroupedTable extends SchemaKGroupedStream {
  private final ExecutionStep<KGroupedTable<Struct, GenericRow>> sourceTableStep;

  SchemaKGroupedTable(
      final ExecutionStep<KGroupedTable<Struct, GenericRow>> sourceTableStep,
      final KeyFormat keyFormat,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KaypherConfig kaypherConfig,
      final FunctionRegistry functionRegistry
  ) {
    super(
        null,
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        kaypherConfig,
        functionRegistry
    );

    this.sourceTableStep = Objects.requireNonNull(sourceTableStep, "sourceTableStep");
  }

  public ExecutionStep<KGroupedTable<Struct, GenericRow>> getSourceTableStep() {
    return sourceTableStep;
  }

  @SuppressWarnings("unchecked")
  @Override
  public SchemaKTable<Struct> aggregate(
      final LogicalSchema aggregateSchema,
      final LogicalSchema outputSchema,
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregations,
      final Optional<WindowExpression> windowExpression,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker,
      final KaypherQueryBuilder queryBuilder
  ) {
    if (windowExpression.isPresent()) {
      throw new KaypherException("Windowing not supported for table aggregations.");
    }

    throwOnValueFieldCountMismatch(outputSchema, nonFuncColumnCount, aggregations);

    final List<String> unsupportedFunctionNames = aggregations.stream()
        .map(call -> UdafUtil.resolveAggregateFunction(
            queryBuilder.getFunctionRegistry(), call, sourceTableStep.getSchema())
        ).filter(function -> !(function instanceof TableAggregationFunction))
        .map(KaypherAggregateFunction::getFunctionName)
        .map(FunctionName::name)
        .collect(Collectors.toList());
    if (!unsupportedFunctionNames.isEmpty()) {
      throw new KaypherException(
          String.format(
            "The aggregation function(s) (%s) cannot be applied to a table.",
            String.join(", ", unsupportedFunctionNames)));
    }

    final TableAggregate step = ExecutionStepFactory.tableAggregate(
        contextStacker,
        sourceTableStep,
        outputSchema,
        Formats.of(keyFormat, valueFormat, SerdeOption.none()),
        nonFuncColumnCount,
        aggregations,
        aggregateSchema
    );

    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        SchemaKStream.Type.AGGREGATE,
        kaypherConfig,
        functionRegistry
    );
  }
}
