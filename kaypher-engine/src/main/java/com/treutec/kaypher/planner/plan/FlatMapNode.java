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

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.analyzer.Analysis;
import com.treutec.kaypher.engine.rewrite.ExpressionTreeRewriter;
import com.treutec.kaypher.engine.rewrite.ExpressionTreeRewriter.Context;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.expression.tree.VisitParentExpressionVisitor;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.execution.util.ExpressionTypeManager;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.structured.SchemaKStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;

/**
 * A node in the logical plan which represents a flat map operation - transforming a single row into
 * zero or more rows.
 */
@Immutable
public class FlatMapNode extends PlanNode {

  private final PlanNode source;
  private final LogicalSchema outputSchema;
  private final List<SelectExpression> finalSelectExpressions;
  private final Analysis analysis;
  private final FunctionRegistry functionRegistry;

  public FlatMapNode(
      final PlanNodeId id,
      final PlanNode source,
      final FunctionRegistry functionRegistry,
      final Analysis analysis
  ) {
    super(id, source.getNodeOutputType());
    this.source = Objects.requireNonNull(source, "source");
    this.analysis = Objects.requireNonNull(analysis);
    this.functionRegistry = functionRegistry;
    this.finalSelectExpressions = buildFinalSelectExpressions();
    outputSchema = buildLogicalSchema(source.getSchema());
  }

  @Override
  public LogicalSchema getSchema() {
    return outputSchema;
  }

  @Override
  public KeyField getKeyField() {
    return source.getKeyField();
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public PlanNode getSource() {
    return source;
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return finalSelectExpressions;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitFlatMap(this, context);
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public SchemaKStream<?> buildStream(final KaypherQueryBuilder builder) {

    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId().toString());

    return getSource().buildStream(builder).flatMap(
        outputSchema,
        analysis.getTableFunctions(),
        contextStacker
    );
  }

  private LogicalSchema buildLogicalSchema(final LogicalSchema inputSchema) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final List<Column> cols = inputSchema.value();

    // We copy all the original columns to the output schema
    schemaBuilder.keyColumns(inputSchema.key());
    for (Column col : cols) {
      schemaBuilder.valueColumn(col);
    }

    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        inputSchema, functionRegistry);

    // And add new columns representing the exploded values at the end
    for (int i = 0; i < analysis.getTableFunctions().size(); i++) {
      final FunctionCall functionCall = analysis.getTableFunctions().get(i);
      final ColumnName colName = ColumnName.synthesisedSchemaColumn(i);
      final SqlType fieldType = expressionTypeManager.getExpressionSqlType(functionCall);
      schemaBuilder.valueColumn(colName, fieldType);
    }

    return schemaBuilder.build();
  }

  private List<SelectExpression> buildFinalSelectExpressions() {
    final TableFunctionExpressionRewriter tableFunctionExpressionRewriter =
        new TableFunctionExpressionRewriter();
    final List<SelectExpression> selectExpressions = new ArrayList<>();
    for (final SelectExpression select : analysis.getSelectExpressions()) {
      final Expression exp = select.getExpression();
      selectExpressions.add(
          SelectExpression.of(
              select.getAlias(),
              ExpressionTreeRewriter.rewriteWith(
                  tableFunctionExpressionRewriter::process, exp)
          ));
    }
    return selectExpressions;
  }

  private class TableFunctionExpressionRewriter
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    private int variableIndex = 0;

    TableFunctionExpressionRewriter() {
      super(Optional.empty());
    }

    @Override
    public Optional<Expression> visitFunctionCall(
        final FunctionCall node,
        final Context<Void> context
    ) {
      final String functionName = node.getName().name();
      if (functionRegistry.isTableFunction(functionName)) {
        final ColumnName varName = ColumnName.synthesisedSchemaColumn(variableIndex);
        variableIndex++;
        return Optional.of(
            new ColumnReferenceExp(node.getLocation(), ColumnRef.of(Optional.empty(), varName)));
      } else {
        final List<Expression> arguments = new ArrayList<>();
        for (final Expression argExpression : node.getArguments()) {
          arguments.add(context.process(argExpression));
        }
        return Optional.of(new FunctionCall(node.getLocation(), node.getName(), arguments));
      }
    }
  }
}
