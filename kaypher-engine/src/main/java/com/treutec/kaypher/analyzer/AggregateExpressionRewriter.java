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
package com.treutec.kaypher.analyzer;

import com.treutec.kaypher.engine.rewrite.ExpressionTreeRewriter;
import com.treutec.kaypher.engine.rewrite.ExpressionTreeRewriter.Context;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.expression.tree.VisitParentExpressionVisitor;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AggregateExpressionRewriter
    extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

  private int aggVariableIndex = 0;
  private final FunctionRegistry functionRegistry;

  public AggregateExpressionRewriter(final FunctionRegistry functionRegistry) {
    super(Optional.empty());
    this.functionRegistry = functionRegistry;
  }

  @Override
  public Optional<Expression> visitFunctionCall(
      final FunctionCall node,
      final ExpressionTreeRewriter.Context<Void> context) {
    final String functionName = node.getName().name();
    if (functionRegistry.isAggregate(functionName)) {
      final ColumnName aggVarName = ColumnName.aggregateColumn(aggVariableIndex);
      aggVariableIndex++;
      return Optional.of(
          new ColumnReferenceExp(node.getLocation(), ColumnRef.withoutSource(aggVarName)));
    } else {
      final List<Expression> arguments = new ArrayList<>();
      for (final Expression argExpression: node.getArguments()) {
        arguments.add(context.process(argExpression));
      }
      return Optional.of(new FunctionCall(node.getLocation(), node.getName(), arguments));
    }
  }
}
