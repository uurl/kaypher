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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.treutec.kaypher.engine.rewrite.ExpressionTreeRewriter;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.name.FunctionName;
import com.treutec.kaypher.parser.tree.Query;
import com.treutec.kaypher.parser.tree.Sink;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.util.KaypherException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryAnalyzer {

  private final Analyzer analyzer;
  private final MetaStore metaStore;
  private final QueryValidator continuousValidator;
  private final QueryValidator staticValidator;

  public QueryAnalyzer(
      final MetaStore metaStore,
      final String outputTopicPrefix,
      final Set<SerdeOption> defaultSerdeOptions
  ) {
    this(
        metaStore,
        new Analyzer(metaStore, outputTopicPrefix, defaultSerdeOptions),
        new ContinuousQueryValidator(),
        new StaticQueryValidator()
    );
  }

  @VisibleForTesting
  QueryAnalyzer(
      final MetaStore metaStore,
      final Analyzer analyzer,
      final QueryValidator continuousValidator,
      final QueryValidator staticValidator
  ) {
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.analyzer = requireNonNull(analyzer, "analyzer");
    this.continuousValidator = requireNonNull(continuousValidator, "continuousValidator");
    this.staticValidator = requireNonNull(staticValidator, "staticValidator");
  }

  public Analysis analyze(
      final Query query,
      final Optional<Sink> sink
  ) {
    final Analysis analysis = analyzer.analyze(query, sink);

    if (query.isStatic()) {
      staticValidator.validate(analysis);
    } else {
      continuousValidator.validate(analysis);
    }

    return analysis;
  }

  public AggregateAnalysis analyzeAggregate(final Query query, final Analysis analysis) {
    final MutableAggregateAnalysis aggregateAnalysis = new MutableAggregateAnalysis();
    final ColumnReferenceExp defaultArgument = analysis.getDefaultArgument();
    final AggregateAnalyzer aggregateAnalyzer =
        new AggregateAnalyzer(aggregateAnalysis, defaultArgument, metaStore);
    final AggregateExpressionRewriter aggregateExpressionRewriter =
        new AggregateExpressionRewriter(metaStore);

    processSelectExpressions(
        analysis,
        aggregateAnalysis,
        aggregateAnalyzer,
        aggregateExpressionRewriter
    );

    if (!aggregateAnalysis.getAggregateFunctions().isEmpty()
        && analysis.getGroupByExpressions().isEmpty()) {
      final String aggFuncs = aggregateAnalysis.getAggregateFunctions().stream()
          .map(FunctionCall::getName)
          .map(FunctionName::name)
          .collect(Collectors.joining(", "));
      throw new KaypherException("Use of aggregate functions requires a GROUP BY clause. "
          + "Aggregate function(s): " + aggFuncs);
    }

    processGroupByExpression(
        analysis,
        aggregateAnalyzer
    );

    analysis.getHavingExpression().ifPresent(having ->
        processHavingExpression(
            having,
            aggregateAnalysis,
            aggregateAnalyzer,
            aggregateExpressionRewriter
        )
    );

    enforceAggregateRules(query, analysis, aggregateAnalysis);
    return aggregateAnalysis;
  }

  private static void processHavingExpression(
      final Expression having,
      final MutableAggregateAnalysis aggregateAnalysis,
      final AggregateAnalyzer aggregateAnalyzer,
      final AggregateExpressionRewriter aggregateExpressionRewriter
  ) {
    aggregateAnalyzer.processHaving(having);

    aggregateAnalysis.setHavingExpression(
        ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter::process, having));
  }

  private static void processGroupByExpression(
      final Analysis analysis,
      final AggregateAnalyzer aggregateAnalyzer
  ) {
    for (final Expression exp : analysis.getGroupByExpressions()) {
      aggregateAnalyzer.processGroupBy(exp);
    }
  }

  private static void processSelectExpressions(
      final Analysis analysis,
      final MutableAggregateAnalysis aggregateAnalysis,
      final AggregateAnalyzer aggregateAnalyzer,
      final AggregateExpressionRewriter aggregateExpressionRewriter
  ) {
    for (final SelectExpression select : analysis.getSelectExpressions()) {
      final Expression exp = select.getExpression();
      aggregateAnalyzer.processSelect(exp);

      aggregateAnalysis.addFinalSelectExpression(
          ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter::process, exp));
    }
  }

  private static void enforceAggregateRules(
      final Query query,
      final Analysis analysis,
      final AggregateAnalysis aggregateAnalysis
  ) {
    if (!query.getGroupBy().isPresent()) {
      return;
    }

    if (!analysis.getTableFunctions().isEmpty()) {
      throw new KaypherException("Table functions cannot be used with aggregations.");
    }

    if (aggregateAnalysis.getAggregateFunctions().isEmpty()) {
      throw new KaypherException(
          "GROUP BY requires columns using aggregate functions in SELECT clause.");
    }

    final Set<Expression> groupByExprs = ImmutableSet.copyOf(analysis.getGroupByExpressions());

    final List<String> unmatchedSelects = aggregateAnalysis.getNonAggregateSelectExpressions()
        .entrySet()
        .stream()
        // Remove any that exactly match a group by expression:
        .filter(e -> !groupByExprs.contains(e.getKey()))
        // Remove any that are constants,
        // or expressions where all params exactly match a group by expression:
        .filter(e -> !Sets.difference(e.getValue(), groupByExprs).isEmpty())
        .map(Map.Entry::getKey)
        .map(Expression::toString)
        .sorted()
        .collect(Collectors.toList());

    if (!unmatchedSelects.isEmpty()) {
      throw new KaypherException(
          "Non-aggregate SELECT expression(s) not part of GROUP BY: " + unmatchedSelects);
    }

    final SetView<ColumnReferenceExp> unmatchedSelectsAgg = Sets
        .difference(aggregateAnalysis.getAggregateSelectFields(), groupByExprs);
    if (!unmatchedSelectsAgg.isEmpty()) {
      throw new KaypherException(
          "Field used in aggregate SELECT expression(s) "
              + "outside of aggregate functions not part of GROUP BY: " + unmatchedSelectsAgg);
    }

    final Set<ColumnReferenceExp> havingColumns = aggregateAnalysis
        .getNonAggregateHavingFields();

    final Set<ColumnReferenceExp> havingOnly = Sets.difference(havingColumns, groupByExprs);
    if (!havingOnly.isEmpty()) {
      throw new KaypherException(
          "Non-aggregate HAVING expression not part of GROUP BY: " + havingOnly);
    }
  }
}
