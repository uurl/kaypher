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

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.expression.tree.TraversalExpressionVisitor;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.util.KaypherException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

class AggregateAnalyzer {

  private final MutableAggregateAnalysis aggregateAnalysis;
  private final ColumnReferenceExp defaultArgument;
  private final FunctionRegistry functionRegistry;

  AggregateAnalyzer(
      final MutableAggregateAnalysis aggregateAnalysis,
      final ColumnReferenceExp defaultArgument,
      final FunctionRegistry functionRegistry
  ) {
    this.aggregateAnalysis = Objects.requireNonNull(aggregateAnalysis, "aggregateAnalysis");
    this.defaultArgument = Objects.requireNonNull(defaultArgument, "defaultArgument");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  void processSelect(final Expression expression) {
    final Set<ColumnReferenceExp> nonAggParams = new HashSet<>();
    final AggregateVisitor visitor = new AggregateVisitor((aggFuncName, node) -> {
      if (!aggFuncName.isPresent()) {
        nonAggParams.add(node);
      }
    });

    visitor.process(expression, null);

    if (visitor.visitedAggFunction) {
      aggregateAnalysis.addAggregateSelectField(nonAggParams);
    } else {
      aggregateAnalysis.addNonAggregateSelectExpression(expression, nonAggParams);
    }
  }

  void processGroupBy(final Expression expression) {
    final AggregateVisitor visitor = new AggregateVisitor((aggFuncName, node) -> {
      if (aggFuncName.isPresent()) {
        throw new KaypherException("GROUP BY does not support aggregate functions: "
            + aggFuncName.get() + " is an aggregate function.");
      }
    });

    visitor.process(expression, null);
  }

  void processHaving(final Expression expression) {
    final AggregateVisitor visitor = new AggregateVisitor((aggFuncName, node) -> {
      if (!aggFuncName.isPresent()) {
        aggregateAnalysis.addNonAggregateHavingField(node);
      }
    });
    visitor.process(expression, null);
  }

  private final class AggregateVisitor extends TraversalExpressionVisitor<Void> {

    private final BiConsumer<Optional<String>, ColumnReferenceExp> dereferenceCollector;
    private Optional<String> aggFunctionName = Optional.empty();
    private boolean visitedAggFunction = false;

    private AggregateVisitor(
        final BiConsumer<Optional<String>, ColumnReferenceExp> dereferenceCollector
    ) {
      this.dereferenceCollector =
          Objects.requireNonNull(dereferenceCollector, "dereferenceCollector");
    }

    @Override
    public Void visitFunctionCall(final FunctionCall node, final Void context) {
      final String functionName = node.getName().name();
      final boolean aggregateFunc = functionRegistry.isAggregate(functionName);

      final FunctionCall functionCall = aggregateFunc && node.getArguments().isEmpty()
          ? new FunctionCall(node.getLocation(), node.getName(), ImmutableList.of(defaultArgument))
          : node;

      if (aggregateFunc) {
        if (aggFunctionName.isPresent()) {
          throw new KaypherException("Aggregate functions can not be nested: "
              + aggFunctionName.get() + "(" + functionName + "())");
        }

        visitedAggFunction = true;
        aggFunctionName = Optional.of(functionName);

        functionCall.getArguments().forEach(aggregateAnalysis::addAggregateFunctionArgument);
        aggregateAnalysis.addAggFunction(functionCall);
      }

      super.visitFunctionCall(functionCall, context);

      if (aggregateFunc) {
        aggFunctionName = Optional.empty();
      }

      return null;
    }

    @Override
    public Void visitColumnReference(
        final ColumnReferenceExp node,
        final Void context
    ) {
      dereferenceCollector.accept(aggFunctionName, node);
      aggregateAnalysis.addRequiredColumn(node);
      return null;
    }
  }
}
