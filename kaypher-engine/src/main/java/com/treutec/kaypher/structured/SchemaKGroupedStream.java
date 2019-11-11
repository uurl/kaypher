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
import com.treutec.kaypher.execution.plan.ExecutionStep;
import com.treutec.kaypher.execution.plan.Formats;
import com.treutec.kaypher.execution.plan.KTableHolder;
import com.treutec.kaypher.execution.streams.ExecutionStepFactory;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.model.WindowType;
import com.treutec.kaypher.parser.tree.WindowExpression;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.serde.WindowInfo;
import com.treutec.kaypher.util.KaypherConfig;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedStream;

public class SchemaKGroupedStream {

  final ExecutionStep<KGroupedStream<Struct, GenericRow>> sourceStep;
  final KeyFormat keyFormat;
  final KeyField keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final KaypherConfig kaypherConfig;
  final FunctionRegistry functionRegistry;

  SchemaKGroupedStream(
      final ExecutionStep<KGroupedStream<Struct, GenericRow>> sourceStep,
      final KeyFormat keyFormat,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final KaypherConfig kaypherConfig,
      final FunctionRegistry functionRegistry
  ) {
    this.sourceStep = sourceStep;
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");
    this.keyField = keyField;
    this.sourceSchemaKStreams = sourceSchemaKStreams;
    this.kaypherConfig = Objects.requireNonNull(kaypherConfig, "kaypherConfig");
    this.functionRegistry = functionRegistry;
  }

  public KeyField getKeyField() {
    return keyField;
  }

  public ExecutionStep<KGroupedStream<Struct, GenericRow>> getSourceStep() {
    return sourceStep;
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable<?> aggregate(
      final LogicalSchema aggregateSchema,
      final LogicalSchema outputSchema,
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregations,
      final Optional<WindowExpression> windowExpression,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker,
      final KaypherQueryBuilder queryBuilder
  ) {
    throwOnValueFieldCountMismatch(outputSchema, nonFuncColumnCount, aggregations);

    final ExecutionStep<? extends KTableHolder<?>> step;
    final KeyFormat keyFormat;

    if (windowExpression.isPresent()) {
      keyFormat = getKeyFormat(windowExpression.get());
      step = ExecutionStepFactory.streamWindowedAggregate(
          contextStacker,
          sourceStep,
          outputSchema,
          Formats.of(keyFormat, valueFormat, SerdeOption.none()),
          nonFuncColumnCount,
          aggregations,
          aggregateSchema,
          windowExpression.get().getKaypherWindowExpression()
      );
    } else {
      keyFormat = this.keyFormat;
      step = ExecutionStepFactory.streamAggregate(
          contextStacker,
          sourceStep,
          outputSchema,
          Formats.of(keyFormat, valueFormat, SerdeOption.none()),
          nonFuncColumnCount,
          aggregations,
          aggregateSchema
      );
    }

    return new SchemaKTable(
        step,
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        SchemaKStream.Type.AGGREGATE,
        kaypherConfig,
        functionRegistry
    );
  }

  private KeyFormat getKeyFormat(final WindowExpression windowExpression) {
    if (kaypherConfig.getBoolean(KaypherConfig.KAYPHER_WINDOWED_SESSION_KEY_LEGACY_CONFIG)) {
      return KeyFormat.windowed(
          FormatInfo.of(Format.KAFKA),
          WindowInfo.of(
              WindowType.TUMBLING,
              Optional.of(Duration.ofMillis(Long.MAX_VALUE))
          )
      );
    }
    return KeyFormat.windowed(
        FormatInfo.of(Format.KAFKA),
        windowExpression.getKaypherWindowExpression().getWindowInfo()
    );
  }

  static void throwOnValueFieldCountMismatch(
      final LogicalSchema aggregateSchema,
      final int nonFuncColumnCount,
      final List<FunctionCall> aggregateFunctions
  ) {
    final int totalColumnCount = aggregateFunctions.size() + nonFuncColumnCount;

    final int valueColumnCount = aggregateSchema.value().size();
    if (valueColumnCount != totalColumnCount) {
      throw new IllegalArgumentException(
          "Aggregate schema value field count does not match expected."
          + " expected: " + totalColumnCount
          + ", actual: " + valueColumnCount
          + ", schema: " + aggregateSchema
      );
    }
  }
}
