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

import static com.treutec.kaypher.execution.streams.ExecutionStepFactory.streamSource;
import static com.treutec.kaypher.execution.streams.ExecutionStepFactory.streamSourceWindowed;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.engine.rewrite.StatementRewriteForRowtime;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.context.QueryLoggerUtil;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.plan.AbstractStreamSource;
import com.treutec.kaypher.execution.plan.ExecutionStep;
import com.treutec.kaypher.execution.plan.ExecutionStepProperties;
import com.treutec.kaypher.execution.plan.Formats;
import com.treutec.kaypher.execution.plan.JoinType;
import com.treutec.kaypher.execution.plan.KStreamHolder;
import com.treutec.kaypher.execution.plan.LogicalSchemaWithMetaAndKeyFields;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.execution.plan.StreamFilter;
import com.treutec.kaypher.execution.plan.StreamFlatMap;
import com.treutec.kaypher.execution.plan.StreamGroupBy;
import com.treutec.kaypher.execution.plan.StreamGroupByKey;
import com.treutec.kaypher.execution.plan.StreamMapValues;
import com.treutec.kaypher.execution.plan.StreamSelectKey;
import com.treutec.kaypher.execution.plan.StreamSink;
import com.treutec.kaypher.execution.plan.StreamSource;
import com.treutec.kaypher.execution.plan.StreamStreamJoin;
import com.treutec.kaypher.execution.plan.StreamTableJoin;
import com.treutec.kaypher.execution.plan.StreamToTable;
import com.treutec.kaypher.execution.plan.WindowedStreamSource;
import com.treutec.kaypher.execution.streams.ExecutionStepFactory;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.util.IdentifierUtil;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.SchemaUtil;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.JoinWindows;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SchemaKStream<K> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final FormatOptions FORMAT_OPTIONS =
      FormatOptions.of(IdentifierUtil::needsQuotes);

  private static final String GROUP_BY_COLUMN_SEPARATOR = "|+|";

  public enum Type { SOURCE, PROJECT, FILTER, AGGREGATE, SINK, REKEY, JOIN }

  final KeyFormat keyFormat;
  final KeyField keyField;
  final List<SchemaKStream> sourceSchemaKStreams;
  final Type type;
  final KaypherConfig kaypherConfig;
  final FunctionRegistry functionRegistry;
  private final ExecutionStep<KStreamHolder<K>> sourceStep;
  private final ExecutionStepProperties sourceProperties;

  private static <K> SchemaKStream<K> forSource(
      final KaypherQueryBuilder builder,
      final KeyFormat keyFormat,
      final AbstractStreamSource<KStreamHolder<K>> streamSource,
      final KeyField keyField) {
    return new SchemaKStream<>(
        streamSource,
        keyFormat,
        keyField,
        ImmutableList.of(),
        SchemaKStream.Type.SOURCE,
        builder.getKaypherConfig(),
        builder.getFunctionRegistry()
    );
  }

  public static SchemaKStream<?> forSource(
      final KaypherQueryBuilder builder,
      final DataSource<?> dataSource,
      final LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
      final QueryContext.Stacker contextStacker,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset,
      final KeyField keyField
  ) {
    final KaypherTopic topic = dataSource.getKaypherTopic();
    if (topic.getKeyFormat().isWindowed()) {
      final WindowedStreamSource step = streamSourceWindowed(
          contextStacker,
          schemaWithMetaAndKeyFields,
          topic.getKafkaTopicName(),
          Formats.of(topic.getKeyFormat(), topic.getValueFormat(), dataSource.getSerdeOptions()),
          dataSource.getTimestampExtractionPolicy(),
          timestampIndex,
          offsetReset
      );
      return forSource(
          builder,
          topic.getKeyFormat(),
          step,
          keyField);
    } else {
      final StreamSource step = streamSource(
          contextStacker,
          schemaWithMetaAndKeyFields,
          topic.getKafkaTopicName(),
          Formats.of(topic.getKeyFormat(), topic.getValueFormat(), dataSource.getSerdeOptions()),
          dataSource.getTimestampExtractionPolicy(),
          timestampIndex,
          offsetReset
      );
      return forSource(
          builder,
          topic.getKeyFormat(),
          step,
          keyField);
    }
  }

  @VisibleForTesting
  SchemaKStream(
      final ExecutionStep<KStreamHolder<K>> sourceStep,
      final KeyFormat keyFormat,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Type type,
      final KaypherConfig kaypherConfig,
      final FunctionRegistry functionRegistry
  ) {
    this(
        requireNonNull(sourceStep, "sourceStep"),
        sourceStep.getProperties(),
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        type,
        kaypherConfig,
        functionRegistry
    );
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumber
  SchemaKStream(
      final ExecutionStep<KStreamHolder<K>> sourceStep,
      final ExecutionStepProperties sourceProperties,
      final KeyFormat keyFormat,
      final KeyField keyField,
      final List<SchemaKStream> sourceSchemaKStreams,
      final Type type,
      final KaypherConfig kaypherConfig,
      final FunctionRegistry functionRegistry
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumber
    this.keyFormat = requireNonNull(keyFormat, "keyFormat");
    this.sourceStep = sourceStep;
    this.sourceProperties = Objects.requireNonNull(sourceProperties, "sourceProperties");
    this.keyField = requireNonNull(keyField, "keyField").validateKeyExistsIn(getSchema());
    this.sourceSchemaKStreams = requireNonNull(sourceSchemaKStreams, "sourceSchemaKStreams");
    this.type = requireNonNull(type, "type");
    this.kaypherConfig = requireNonNull(kaypherConfig, "kaypherConfig");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
  }

  public SchemaKTable<K> toTable(
      final KeyFormat keyFormat,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker
  ) {
    final StreamToTable<K> step = ExecutionStepFactory.streamToTable(
        contextStacker,
        Formats.of(keyFormat, valueFormat, Collections.emptySet()),
        sourceStep
    );
    return new SchemaKTable<>(
        step,
        keyFormat,
        keyField,
        Collections.singletonList(this),
        type,
        kaypherConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> into(
      final String kafkaTopicName,
      final LogicalSchema outputSchema,
      final ValueFormat valueFormat,
      final Set<SerdeOption> options,
      final QueryContext.Stacker contextStacker
  ) {
    final StreamSink<K> step = ExecutionStepFactory.streamSink(
        contextStacker,
        outputSchema,
        Formats.of(keyFormat, valueFormat, options),
        sourceStep,
        kafkaTopicName
    );
    return new SchemaKStream<>(
        step,
        keyFormat,
        keyField,
        Collections.singletonList(this),
        SchemaKStream.Type.SINK,
        kaypherConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> filter(
      final Expression filterExpression,
      final QueryContext.Stacker contextStacker
  ) {
    final StreamFilter<K> step = ExecutionStepFactory.streamFilter(
        contextStacker,
        sourceStep,
        rewriteTimeComparisonForFilter(filterExpression)
    );
    return new SchemaKStream<>(
        step,
        keyFormat,
        keyField,
        Collections.singletonList(this),
        Type.FILTER,
        kaypherConfig,
        functionRegistry
    );
  }

  static Expression rewriteTimeComparisonForFilter(final Expression expression) {
    return new StatementRewriteForRowtime()
        .rewriteForRowtime(expression);
  }

  public SchemaKStream<K> select(
      final List<SelectExpression> selectExpressions,
      final QueryContext.Stacker contextStacker,
      final KaypherQueryBuilder kaypherQueryBuilder) {
    final KeySelection selection = new KeySelection(selectExpressions);
    final StreamMapValues<K> step = ExecutionStepFactory.streamMapValues(
        contextStacker,
        sourceStep,
        selectExpressions,
        kaypherQueryBuilder
    );
    return new SchemaKStream<>(
        step,
        keyFormat,
        selection.getKey(),
        Collections.singletonList(this),
        Type.PROJECT,
        kaypherConfig,
        functionRegistry
    );
  }

  class KeySelection {

    private final KeyField key;

    KeySelection(final List<SelectExpression> selectExpressions) {
      this.key = findKeyField(selectExpressions);
    }

    private KeyField findKeyField(final List<SelectExpression> selectExpressions) {
      if (!getKeyField().ref().isPresent()) {
        return KeyField.none();
      }

      final ColumnRef reference = getKeyField().ref().get();
      final Column keyColumn = Column.of(reference, SqlTypes.STRING);

      Optional<Column> found = Optional.empty();

      for (int i = 0; i < selectExpressions.size(); i++) {
        final ColumnName toName = selectExpressions.get(i).getAlias();
        final Expression toExpression = selectExpressions.get(i).getExpression();

        if (toExpression instanceof ColumnReferenceExp) {
          final ColumnReferenceExp nameRef
              = (ColumnReferenceExp) toExpression;

          if (keyColumn.ref().equals(nameRef.getReference())) {
            found = Optional.of(Column.of(toName, keyColumn.type()));
            break;
          }
        }
      }

      final Optional<ColumnRef> filtered = found
          .filter(f -> !SchemaUtil.isFieldName(f.name().name(), SchemaUtil.ROWTIME_NAME.name()))
          .filter(f -> !SchemaUtil.isFieldName(f.name().name(), SchemaUtil.ROWKEY_NAME.name()))
          .map(Column::ref);

      return KeyField.of(filtered);
    }

    public KeyField getKey() {
      return key;
    }
  }

  public SchemaKStream<K> leftJoin(
      final SchemaKTable<K> schemaKTable,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker
  ) {
    final StreamTableJoin<K> step = ExecutionStepFactory.streamTableJoin(
        contextStacker,
        JoinType.LEFT,
        Formats.of(keyFormat, valueFormat, SerdeOption.none()),
        sourceStep,
        schemaKTable.getSourceTableStep(),
        joinSchema
    );
    return new SchemaKStream<>(
        step,
        keyFormat,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        kaypherConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> leftJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
      final QueryContext.Stacker contextStacker) {

    final StreamStreamJoin<K> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.LEFT,
        Formats.of(keyFormat, leftFormat, SerdeOption.none()),
        Formats.of(keyFormat, rightFormat, SerdeOption.none()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinSchema,
        joinWindows
    );
    return new SchemaKStream<>(
        step,
        keyFormat,
        keyField,
        ImmutableList.of(this, otherSchemaKStream),
        Type.JOIN,
        kaypherConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> join(
      final SchemaKTable<K> schemaKTable,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker
  ) {
    final StreamTableJoin<K> step = ExecutionStepFactory.streamTableJoin(
        contextStacker,
        JoinType.INNER,
        Formats.of(keyFormat, valueFormat, SerdeOption.none()),
        sourceStep,
        schemaKTable.getSourceTableStep(),
        joinSchema
    );
    return new SchemaKStream<>(
        step,
        keyFormat,
        keyField,
        ImmutableList.of(this, schemaKTable),
        Type.JOIN,
        kaypherConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> join(
      final SchemaKStream<K> otherSchemaKStream,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
      final QueryContext.Stacker contextStacker) {
    final StreamStreamJoin<K> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.INNER,
        Formats.of(keyFormat, leftFormat, SerdeOption.none()),
        Formats.of(keyFormat, rightFormat, SerdeOption.none()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinSchema,
        joinWindows
    );
    return new SchemaKStream<>(
        step,
        keyFormat,
        keyField,
        ImmutableList.of(this, otherSchemaKStream),
        Type.JOIN,
        kaypherConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> outerJoin(
      final SchemaKStream<K> otherSchemaKStream,
      final LogicalSchema joinSchema,
      final KeyField keyField,
      final JoinWindows joinWindows,
      final ValueFormat leftFormat,
      final ValueFormat rightFormat,
      final QueryContext.Stacker contextStacker) {
    final StreamStreamJoin<K> step = ExecutionStepFactory.streamStreamJoin(
        contextStacker,
        JoinType.OUTER,
        Formats.of(keyFormat, leftFormat, SerdeOption.none()),
        Formats.of(keyFormat, rightFormat, SerdeOption.none()),
        sourceStep,
        otherSchemaKStream.sourceStep,
        joinSchema,
        joinWindows
    );
    return new SchemaKStream<>(
        step,
        keyFormat,
        keyField,
        ImmutableList.of(this, otherSchemaKStream),
        Type.JOIN,
        kaypherConfig,
        functionRegistry
    );
  }

  @SuppressWarnings("unchecked")
  public SchemaKStream<Struct> selectKey(
      final ColumnRef columnRef,
      final boolean updateRowKey,
      final QueryContext.Stacker contextStacker
  ) {
    if (keyFormat.isWindowed()) {
      throw new UnsupportedOperationException("Can not selectKey of windowed stream");
    }

    final Optional<Column> existingKey = keyField.resolve(getSchema());

    final Column proposedKey = getSchema().findValueColumn(columnRef)
        .orElseThrow(IllegalArgumentException::new);

    final KeyField resultantKeyField = isRowKey(columnRef)
        ? keyField
        : KeyField.of(columnRef);

    final boolean namesMatch = existingKey
        .map(kf -> kf.ref().equals(proposedKey.ref()))
        .orElse(false);

    if (namesMatch || isRowKey(proposedKey.ref())) {
      return (SchemaKStream<Struct>) new SchemaKStream<>(
          sourceStep,
          keyFormat,
          resultantKeyField,
          sourceSchemaKStreams,
          type,
          kaypherConfig,
          functionRegistry
      );
    }

    final KeyField newKeyField = getSchema().isMetaColumn(columnRef.name())
        ? KeyField.none()
        : resultantKeyField;

    final StreamSelectKey<K> step = ExecutionStepFactory.streamSelectKey(
        contextStacker,
        sourceStep,
        columnRef,
        updateRowKey
    );
    return new SchemaKStream<>(
        step,
        keyFormat,
        newKeyField,
        Collections.singletonList(this),
        Type.REKEY,
        kaypherConfig,
        functionRegistry
    );
  }

  private static boolean isRowKey(final ColumnRef fieldName) {
    return fieldName.name().equals(SchemaUtil.ROWKEY_NAME);
  }

  private static ColumnName fieldNameFromExpression(final Expression expression) {
    if (expression instanceof ColumnReferenceExp) {
      final ColumnReferenceExp nameRef = (ColumnReferenceExp) expression;
      return nameRef.getReference().name();
    }
    return null;
  }

  private boolean rekeyRequired(final List<Expression> groupByExpressions) {
    if (groupByExpressions.size() != 1) {
      return true;
    }

    final ColumnName groupByField = fieldNameFromExpression(groupByExpressions.get(0));
    if (groupByField == null) {
      return true;
    }

    if (groupByField.equals(SchemaUtil.ROWKEY_NAME)) {
      return false;
    }

    final Optional<Column> keyColumn = getKeyField().resolve(getSchema());
    if (!keyColumn.isPresent()) {
      return true;
    }

    final ColumnName keyFieldName = keyColumn.get().name();
    return !groupByField.equals(keyFieldName);
  }

  public SchemaKGroupedStream groupBy(
      final ValueFormat valueFormat,
      final List<Expression> groupByExpressions,
      final QueryContext.Stacker contextStacker
  ) {
    final boolean rekey = rekeyRequired(groupByExpressions);
    final KeyFormat rekeyedKeyFormat = KeyFormat.nonWindowed(keyFormat.getFormatInfo());
    if (!rekey) {
      return groupByKey(rekeyedKeyFormat, valueFormat, contextStacker);
    }

    final ColumnRef aggregateKeyName = groupedKeyNameFor(groupByExpressions);

    final Optional<ColumnRef> newKeyCol = getSchema()
        .findValueColumn(aggregateKeyName)
        .map(Column::ref);

    final StreamGroupBy<K> source = ExecutionStepFactory.streamGroupBy(
        contextStacker,
        sourceStep,
        Formats.of(rekeyedKeyFormat, valueFormat, SerdeOption.none()),
        groupByExpressions
    );
    return new SchemaKGroupedStream(
        source,
        rekeyedKeyFormat,
        KeyField.of(newKeyCol),
        Collections.singletonList(this),
        kaypherConfig,
        functionRegistry
    );
  }

  @SuppressWarnings("unchecked")
  private SchemaKGroupedStream groupByKey(
      final KeyFormat rekeyedKeyFormat,
      final ValueFormat valueFormat,
      final QueryContext.Stacker contextStacker
  ) {
    if (keyFormat.isWindowed()) {
      throw new UnsupportedOperationException("Group by on windowed should always require rekey");
    }
    final StreamGroupByKey step =
        ExecutionStepFactory.streamGroupByKey(
            contextStacker,
            (ExecutionStep) sourceStep,
            Formats.of(rekeyedKeyFormat, valueFormat, SerdeOption.none())
        );
    return new SchemaKGroupedStream(
        step,
        keyFormat,
        keyField,
        Collections.singletonList(this),
        kaypherConfig,
        functionRegistry
    );
  }

  public SchemaKStream<K> flatMap(
      final LogicalSchema outputSchema,
      final List<FunctionCall> tableFunctions,
      final QueryContext.Stacker contextStacker
  ) {
    final StreamFlatMap<K> step = ExecutionStepFactory.streamFlatMap(
        contextStacker,
        sourceStep,
        outputSchema,
        tableFunctions
    );
    return new SchemaKStream<>(
        step,
        keyFormat,
        keyField,
        sourceSchemaKStreams,
        type,
        kaypherConfig,
        functionRegistry);
  }

  public ExecutionStep<?> getSourceStep() {
    return sourceStep;
  }

  public KeyField getKeyField() {
    return keyField;
  }

  public QueryContext getQueryContext() {
    return sourceProperties.getQueryContext();
  }

  public LogicalSchema getSchema() {
    return sourceProperties.getSchema();
  }

  public List<SchemaKStream> getSourceSchemaKStreams() {
    return sourceSchemaKStreams;
  }

  public String getExecutionPlan(final QueryId queryId, final String indent) {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(indent)
        .append(" > [ ")
        .append(type).append(" ] | Schema: ")
        .append(getSchema().toString(FORMAT_OPTIONS))
        .append(" | Logger: ").append(QueryLoggerUtil.queryLoggerName(queryId, getQueryContext()))
        .append("\n");
    for (final SchemaKStream schemaKStream : sourceSchemaKStreams) {
      stringBuilder
          .append("\t")
          .append(indent)
          .append(schemaKStream.getExecutionPlan(queryId, indent + "\t"));
    }
    return stringBuilder.toString();
  }

  public Type getType() {
    return type;
  }

  public KeyFormat getKeyFormat() {
    return keyFormat;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  static ColumnRef groupedKeyNameFor(final List<Expression> groupByExpressions) {
    if (groupByExpressions.size() == 1 && groupByExpressions.get(0) instanceof ColumnReferenceExp) {
      return ((ColumnReferenceExp) groupByExpressions.get(0)).getReference();
    }

    // this is safe because if we group by multiple fields the original field
    // will never be in the original schema, so we're necessarily creating a
    // new field
    return ColumnRef.withoutSource(
        ColumnName.of(groupByExpressions.stream()
            .map(Expression::toString)
            .collect(Collectors.joining(GROUP_BY_COLUMN_SEPARATOR))));
  }
}
