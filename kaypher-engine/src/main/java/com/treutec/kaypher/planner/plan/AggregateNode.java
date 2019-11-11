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
package com.treutec.kaypher.planner.plan;

import static com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.engine.rewrite.ExpressionTreeRewriter;
import com.treutec.kaypher.engine.rewrite.ExpressionTreeRewriter.Context;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.expression.tree.Literal;
import com.treutec.kaypher.execution.expression.tree.VisitParentExpressionVisitor;
import com.treutec.kaypher.execution.function.UdafUtil;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.function.KaypherAggregateFunction;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.parser.tree.WindowExpression;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.SchemaConverters;
import com.treutec.kaypher.schema.kaypher.SchemaConverters.ConnectToSqlTypeConverter;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.structured.SchemaKGroupedStream;
import com.treutec.kaypher.structured.SchemaKStream;
import com.treutec.kaypher.structured.SchemaKTable;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.SchemaUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


public class AggregateNode extends PlanNode {

  private static final String INTERNAL_COLUMN_NAME_PREFIX = "KAYPHER_INTERNAL_COL_";

  private static final String PREPARE_OP_NAME = "prepare";
  private static final String AGGREGATION_OP_NAME = "aggregate";
  private static final String GROUP_BY_OP_NAME = "groupby";
  private static final String FILTER_OP_NAME = "filter";
  private static final String PROJECT_OP_NAME = "project";

  private final PlanNode source;
  private final LogicalSchema schema;
  private final KeyField keyField;
  private final List<Expression> groupByExpressions;
  private final Optional<WindowExpression> windowExpression;
  private final List<Expression> aggregateFunctionArguments;
  private final List<FunctionCall> functionList;
  private final List<ColumnReferenceExp> requiredColumns;
  private final List<Expression> finalSelectExpressions;
  private final Expression havingExpressions;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public AggregateNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final Optional<ColumnRef> keyFieldName,
      final List<Expression> groupByExpressions,
      final Optional<WindowExpression> windowExpression,
      final List<Expression> aggregateFunctionArguments,
      final List<FunctionCall> functionList,
      final List<ColumnReferenceExp> requiredColumns,
      final List<Expression> finalSelectExpressions,
      final Expression havingExpressions
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    super(id, DataSourceType.KTABLE);

    this.source = requireNonNull(source, "source");
    this.schema = requireNonNull(schema, "schema");
    this.groupByExpressions = requireNonNull(groupByExpressions, "groupByExpressions");
    this.windowExpression = requireNonNull(windowExpression, "windowExpression");
    this.aggregateFunctionArguments =
        requireNonNull(aggregateFunctionArguments, "aggregateFunctionArguments");
    this.functionList = requireNonNull(functionList, "functionList");
    this.requiredColumns =
        ImmutableList.copyOf(requireNonNull(requiredColumns, "requiredColumns"));
    this.finalSelectExpressions =
        requireNonNull(finalSelectExpressions, "finalSelectExpressions");
    this.havingExpressions = havingExpressions;
    this.keyField = KeyField.of(requireNonNull(keyFieldName, "keyFieldName"))
        .validateKeyExistsIn(schema);
  }

  @Override
  public LogicalSchema getSchema() {
    return this.schema;
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public PlanNode getSource() {
    return source;
  }

  public List<Expression> getGroupByExpressions() {
    return groupByExpressions;
  }

  public Optional<WindowExpression> getWindowExpression() {
    return windowExpression;
  }

  public List<Expression> getAggregateFunctionArguments() {
    return aggregateFunctionArguments;
  }

  public List<FunctionCall> getFunctionCalls() {
    return functionList;
  }

  public List<ColumnReferenceExp> getRequiredColumns() {
    return requiredColumns;
  }

  private List<SelectExpression> getFinalSelectExpressions() {
    final List<SelectExpression> finalSelectExpressionList = new ArrayList<>();
    if (finalSelectExpressions.size() != schema.value().size()) {
      throw new RuntimeException(
          "Incompatible aggregate schema, field count must match, "
              + "selected field count:"
              + finalSelectExpressions.size()
              + " schema field count:"
              + schema.value().size());
    }
    for (int i = 0; i < finalSelectExpressions.size(); i++) {
      finalSelectExpressionList.add(SelectExpression.of(
          schema.value().get(i).name(),
          finalSelectExpressions.get(i)
      ));
    }
    return finalSelectExpressionList;
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return Collections.emptyList();
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitAggregate(this, context);
  }

  @Override
  public SchemaKStream<?> buildStream(final KaypherQueryBuilder builder) {
    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId().toString());
    final DataSourceNode streamSourceNode = getTheSourceNode();
    final SchemaKStream<?> sourceSchemaKStream = getSource().buildStream(builder);

    // Pre aggregate computations
    final InternalSchema internalSchema = new InternalSchema(getRequiredColumns(),
        getAggregateFunctionArguments());

    final SchemaKStream<?> aggregateArgExpanded =
        sourceSchemaKStream.select(
            internalSchema.getAggArgExpansionList(),
            contextStacker.push(PREPARE_OP_NAME),
            builder);

    // This is the schema used in any repartition topic
    // It contains only the fields from the source that are needed by the aggregation
    // It uses internal column names, e.g. KAYPHER_INTERNAL_COL_0
    final LogicalSchema prepareSchema = aggregateArgExpanded.getSchema();

    final QueryContext.Stacker groupByContext = contextStacker.push(GROUP_BY_OP_NAME);

    final ValueFormat valueFormat = streamSourceNode
        .getDataSource()
        .getKaypherTopic()
        .getValueFormat();

    final List<Expression> internalGroupByColumns = internalSchema.resolveGroupByExpressions(
        getGroupByExpressions(),
        aggregateArgExpanded,
        builder.getKaypherConfig()
    );

    final SchemaKGroupedStream schemaKGroupedStream = aggregateArgExpanded.groupBy(
        valueFormat,
        internalGroupByColumns,
        groupByContext
    );

    final List<FunctionCall> functionsWithInternalIdentifiers = functionList.stream()
        .map(
            fc -> new FunctionCall(
                fc.getName(),
                internalSchema.getInternalArgsExpressionList(fc.getArguments())
            )
        )
        .collect(Collectors.toList());

    // This is the schema of the aggregation change log topic and associated state store.
    // It contains all columns from prepareSchema and columns for any aggregating functions
    // It uses internal column names, e.g. KAYPHER_INTERNAL_COL_0 and KAYPHER_AGG_VARIABLE_0
    final LogicalSchema aggregationSchema = buildLogicalSchema(
        prepareSchema,
        functionsWithInternalIdentifiers,
        builder.getFunctionRegistry(),
        true
    );

    final QueryContext.Stacker aggregationContext = contextStacker.push(AGGREGATION_OP_NAME);

    // This is the schema post any {@link Udaf#map} steps to reduce intermediate aggregate state
    // to the final output state
    final LogicalSchema outputSchema = buildLogicalSchema(
        prepareSchema,
        functionsWithInternalIdentifiers,
        builder.getFunctionRegistry(),
        false
    );

    SchemaKTable<?> aggregated = schemaKGroupedStream.aggregate(
        aggregationSchema,
        outputSchema,
        requiredColumns.size(),
        functionsWithInternalIdentifiers,
        windowExpression,
        valueFormat,
        aggregationContext,
        builder
    );

    final Optional<Expression> havingExpression = Optional.ofNullable(havingExpressions)
        .map(internalSchema::resolveToInternal);

    if (havingExpression.isPresent()) {
      aggregated = aggregated.filter(havingExpression.get(), contextStacker.push(FILTER_OP_NAME));
    }

    final List<SelectExpression> finalSelects = internalSchema
        .updateFinalSelectExpressions(getFinalSelectExpressions());

    return aggregated.select(
        finalSelects,
        contextStacker.push(PROJECT_OP_NAME),
        builder);
  }

  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  private LogicalSchema buildLogicalSchema(
      final LogicalSchema inputSchema,
      final List<FunctionCall> aggregations,
      final FunctionRegistry functionRegistry,
      final boolean useAggregate
  ) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final List<Column> cols = inputSchema.value();

    schemaBuilder.keyColumns(inputSchema.key());

    for (int i = 0; i < requiredColumns.size(); i++) {
      schemaBuilder.valueColumn(cols.get(i));
    }

    final ConnectToSqlTypeConverter converter = SchemaConverters.connectToSqlConverter();

    for (int i = 0; i < aggregations.size(); i++) {
      final KaypherAggregateFunction aggregateFunction =
          UdafUtil.resolveAggregateFunction(functionRegistry, aggregations.get(i), inputSchema);
      final ColumnName colName = ColumnName.aggregateColumn(i);
      final SqlType fieldType = converter.toSqlType(
          useAggregate ? aggregateFunction.getAggregateType() : aggregateFunction.getReturnType()
      );
      schemaBuilder.valueColumn(colName, fieldType);
    }

    return schemaBuilder.build();
  }

  private static class InternalSchema {

    private final List<SelectExpression> aggArgExpansions = new ArrayList<>();
    private final Map<String, ColumnName> expressionToInternalColumnName = new HashMap<>();

    InternalSchema(
        final List<ColumnReferenceExp> requiredColumns,
        final List<Expression> aggregateFunctionArguments) {
      final Set<String> seen = new HashSet<>();
      collectAggregateArgExpressions(requiredColumns, seen);
      collectAggregateArgExpressions(aggregateFunctionArguments, seen);
    }

    private void collectAggregateArgExpressions(
        final Collection<? extends Expression> expressions,
        final Set<String> seen
    ) {
      for (final Expression expression : expressions) {
        if (seen.contains(expression.toString())) {
          continue;
        }

        seen.add(expression.toString());

        final String internalName = INTERNAL_COLUMN_NAME_PREFIX + aggArgExpansions.size();

        aggArgExpansions.add(SelectExpression.of(ColumnName.of(internalName), expression));
        expressionToInternalColumnName
            .putIfAbsent(expression.toString(), ColumnName.of(internalName));
      }
    }

    List<Expression> resolveGroupByExpressions(
        final List<Expression> expressionList,
        final SchemaKStream<?> aggregateArgExpanded,
        final KaypherConfig kaypherConfig
    ) {
      final boolean specialRowTimeHandling = !(aggregateArgExpanded instanceof SchemaKTable)
          && !kaypherConfig.getBoolean(KaypherConfig.KAYPHER_LEGACY_REPARTITION_ON_GROUP_BY_ROWKEY);

      final Function<Expression, Expression> mapper = e -> {
        final boolean rowKey = e instanceof ColumnReferenceExp
            && ((ColumnReferenceExp) e).getReference().name().equals(SchemaUtil.ROWKEY_NAME);

        if (!rowKey || !specialRowTimeHandling) {
          return resolveToInternal(e);
        }

        final ColumnReferenceExp nameRef = (ColumnReferenceExp) e;
        return new ColumnReferenceExp(
            nameRef.getLocation(),
            nameRef.getReference().withoutSource()
        );
      };

      return expressionList.stream()
          .map(mapper)
          .collect(Collectors.toList());
    }

    /**
     * Return the aggregate function arguments based on the internal expressions.
     * Currently we support aggregate functions with at most two arguments where
     * the second argument should be a literal.
     * @param argExpressionList The list of parameters for the aggregate fuunction.
     * @return The list of arguments based on the internal expressions for the aggregate function.
     */
    List<Expression> getInternalArgsExpressionList(final List<Expression> argExpressionList) {
      // Currently we only support aggregations on one column only
      if (argExpressionList.size() > 2) {
        throw new KaypherException("Currently, KAYPHER UDAFs can only have two arguments.");
      }
      if (argExpressionList.isEmpty()) {
        return Collections.emptyList();
      }
      final List<Expression> internalExpressionList = new ArrayList<>();
      internalExpressionList.add(resolveToInternal(argExpressionList.get(0)));
      if (argExpressionList.size() == 2) {
        if (! (argExpressionList.get(1) instanceof Literal)) {
          throw new KaypherException("Currently, second argument in UDAF should be literal.");
        }
        internalExpressionList.add(argExpressionList.get(1));
      }
      return internalExpressionList;

    }

    List<SelectExpression> updateFinalSelectExpressions(
        final List<SelectExpression> finalSelectExpressions
    ) {
      return finalSelectExpressions.stream()
          .map(finalSelectExpression -> {
            final Expression internal = resolveToInternal(finalSelectExpression.getExpression());
            return SelectExpression.of(finalSelectExpression.getAlias(), internal);
          })
          .collect(Collectors.toList());
    }

    List<SelectExpression> getAggArgExpansionList() {
      return aggArgExpansions;
    }

    private Expression resolveToInternal(final Expression exp) {
      final ColumnName name = expressionToInternalColumnName.get(exp.toString());
      if (name != null) {
        return new ColumnReferenceExp(
            exp.getLocation(),
            ColumnRef.withoutSource(name));
      }

      return ExpressionTreeRewriter.rewriteWith(new ResolveToInternalRewriter()::process, exp);
    }

    private final class ResolveToInternalRewriter
        extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {
      private ResolveToInternalRewriter() {
        super(Optional.empty());
      }

      @Override
      public Optional<Expression> visitColumnReference(
          final ColumnReferenceExp node,
          final Context<Void> context
      ) {
        // internal names are source-less
        final ColumnName name = expressionToInternalColumnName.get(node.toString());
        if (name != null) {
          return Optional.of(
              new ColumnReferenceExp(
                  node.getLocation(),
                  ColumnRef.withoutSource(name)));
        }

        final boolean isAggregate = node.getReference().name().isAggregate();

        if (!isAggregate || node.getReference().source().isPresent()) {
          throw new KaypherException("Unknown source column: " + node.toString());
        }

        return Optional.of(node);
      }
    }
  }
}
