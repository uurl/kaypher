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
package com.treutec.kaypher.planner;

import com.treutec.kaypher.analyzer.AggregateAnalysisResult;
import com.treutec.kaypher.analyzer.Analysis;
import com.treutec.kaypher.analyzer.Analysis.AliasedDataSource;
import com.treutec.kaypher.analyzer.Analysis.Into;
import com.treutec.kaypher.analyzer.Analysis.JoinInfo;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.execution.util.ExpressionTypeManager;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.planner.plan.AggregateNode;
import com.treutec.kaypher.planner.plan.DataSourceNode;
import com.treutec.kaypher.planner.plan.FilterNode;
import com.treutec.kaypher.planner.plan.FlatMapNode;
import com.treutec.kaypher.planner.plan.JoinNode;
import com.treutec.kaypher.planner.plan.KaypherBareOutputNode;
import com.treutec.kaypher.planner.plan.KaypherStructuredDataOutputNode;
import com.treutec.kaypher.planner.plan.OutputNode;
import com.treutec.kaypher.planner.plan.PlanNode;
import com.treutec.kaypher.planner.plan.PlanNodeId;
import com.treutec.kaypher.planner.plan.ProjectNode;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.LogicalSchema.Builder;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.SchemaUtil;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicy;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicyFactory;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class LogicalPlanner {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final KaypherConfig kaypherConfig;
  private final Analysis analysis;
  private final AggregateAnalysisResult aggregateAnalysis;
  private final FunctionRegistry functionRegistry;

  public LogicalPlanner(
      final KaypherConfig kaypherConfig,
      final Analysis analysis,
      final AggregateAnalysisResult aggregateAnalysis,
      final FunctionRegistry functionRegistry
  ) {
    this.kaypherConfig = kaypherConfig;
    this.analysis = analysis;
    this.aggregateAnalysis = aggregateAnalysis;
    this.functionRegistry = functionRegistry;
  }

  public OutputNode buildPlan() {
    PlanNode currentNode = buildSourceNode();

    if (analysis.getWhereExpression().isPresent()) {
      currentNode = buildFilterNode(currentNode, analysis.getWhereExpression().get());
    }

    if (!analysis.getTableFunctions().isEmpty()) {
      currentNode = buildFlatMapNode(currentNode);
    }

    if (analysis.getGroupByExpressions().isEmpty()) {
      currentNode = buildProjectNode(currentNode);
    } else {
      currentNode = buildAggregateNode(currentNode);
    }

    return buildOutputNode(currentNode);
  }

  private OutputNode buildOutputNode(final PlanNode sourcePlanNode) {
    final LogicalSchema inputSchema = sourcePlanNode.getSchema();
    final TimestampExtractionPolicy extractionPolicy =
        getTimestampExtractionPolicy(inputSchema, analysis);

    if (!analysis.getInto().isPresent()) {
      return new KaypherBareOutputNode(
          new PlanNodeId("KAYPHER_STDOUT_NAME"),
          sourcePlanNode,
          inputSchema,
          analysis.getLimitClause(),
          extractionPolicy
      );
    }

    final Into intoDataSource = analysis.getInto().get();

    final Optional<ColumnRef> partitionByField = analysis.getPartitionBy();

    partitionByField.ifPresent(keyName ->
        inputSchema.findValueColumn(keyName)
            .orElseThrow(() -> new KaypherException(
                "Column " + keyName.name().toString(FormatOptions.noEscape())
                    + " does not exist in the result schema. Error in Partition By clause.")
            ));

    final KeyField keyField = buildOutputKeyField(sourcePlanNode);

    return new KaypherStructuredDataOutputNode(
        new PlanNodeId(intoDataSource.getName().name()),
        sourcePlanNode,
        inputSchema,
        extractionPolicy,
        keyField,
        intoDataSource.getKaypherTopic(),
        partitionByField,
        analysis.getLimitClause(),
        intoDataSource.isCreate(),
        analysis.getSerdeOptions(),
        intoDataSource.getName()
    );
  }

  private KeyField buildOutputKeyField(
      final PlanNode sourcePlanNode
  ) {
    final KeyField sourceKeyField = sourcePlanNode.getKeyField();

    final Optional<ColumnRef> partitionByField = analysis.getPartitionBy();
    if (!partitionByField.isPresent()) {
      return sourceKeyField;
    }

    final ColumnRef partitionBy = partitionByField.get();
    final LogicalSchema schema = sourcePlanNode.getSchema();

    if (schema.isMetaColumn(partitionBy.name())) {
      return KeyField.none();
    }

    if (schema.isKeyColumn(partitionBy.name())) {
      return sourceKeyField;
    }

    return KeyField.of(partitionBy);
  }

  private TimestampExtractionPolicy getTimestampExtractionPolicy(
      final LogicalSchema inputSchema,
      final Analysis analysis
  ) {
    return TimestampExtractionPolicyFactory.create(
        kaypherConfig,
        inputSchema,
        analysis.getProperties().getTimestampColumnName(),
        analysis.getProperties().getTimestampFormat());
  }

  private AggregateNode buildAggregateNode(final PlanNode sourcePlanNode) {
    final Expression groupBy = analysis.getGroupByExpressions().size() == 1
        ? analysis.getGroupByExpressions().get(0)
        : null;

    final LogicalSchema schema = buildProjectionSchema(sourcePlanNode);

    final Optional<ColumnName> keyFieldName = getSelectAliasMatching((expression, alias) ->
        expression.equals(groupBy)
            && !SchemaUtil.isFieldName(alias.name(), SchemaUtil.ROWTIME_NAME.name())
            && !SchemaUtil.isFieldName(alias.name(), SchemaUtil.ROWKEY_NAME.name()),
        sourcePlanNode);

    return new AggregateNode(
        new PlanNodeId("Aggregate"),
        sourcePlanNode,
        schema,
        keyFieldName.map(ColumnRef::withoutSource),
        analysis.getGroupByExpressions(),
        analysis.getWindowExpression(),
        aggregateAnalysis.getAggregateFunctionArguments(),
        aggregateAnalysis.getAggregateFunctions(),
        aggregateAnalysis.getRequiredColumns(),
        aggregateAnalysis.getFinalSelectExpressions(),
        aggregateAnalysis.getHavingExpression()
    );
  }

  private ProjectNode buildProjectNode(final PlanNode sourcePlanNode) {
    final ColumnRef sourceKeyFieldName = sourcePlanNode
        .getKeyField()
        .ref()
        .orElse(null);

    final LogicalSchema schema = buildProjectionSchema(sourcePlanNode);

    final Optional<ColumnName> keyFieldName = getSelectAliasMatching((expression, alias) ->
        expression instanceof ColumnReferenceExp
            && ((ColumnReferenceExp) expression).getReference().equals(sourceKeyFieldName),
        sourcePlanNode
    );

    return new ProjectNode(
        new PlanNodeId("Project"),
        sourcePlanNode,
        schema,
        keyFieldName.map(ColumnRef::withoutSource)
    );
  }

  private static FilterNode buildFilterNode(
      final PlanNode sourcePlanNode,
      final Expression filterExpression
  ) {
    return new FilterNode(new PlanNodeId("Filter"), sourcePlanNode, filterExpression);
  }

  private FlatMapNode buildFlatMapNode(final PlanNode sourcePlanNode) {
    return new FlatMapNode(new PlanNodeId("FlatMap"), sourcePlanNode, functionRegistry, analysis);
  }

  private PlanNode buildSourceNode() {

    final List<AliasedDataSource> sources = analysis.getFromDataSources();

    final Optional<JoinInfo> joinInfo = analysis.getJoin();
    if (!joinInfo.isPresent()) {
      return buildNonJoinNode(sources);
    }

    if (sources.size() != 2) {
      throw new IllegalStateException("Expected 2 sources. Got " + sources.size());
    }

    final AliasedDataSource left = sources.get(0);
    final AliasedDataSource right = sources.get(1);

    final DataSourceNode leftSourceNode = new DataSourceNode(
        new PlanNodeId("KafkaTopic_Left"),
        left.getDataSource(),
        left.getAlias(),
        analysis.getSelectExpressions()
    );

    final DataSourceNode rightSourceNode = new DataSourceNode(
        new PlanNodeId("KafkaTopic_Right"),
        right.getDataSource(),
        right.getAlias(),
        analysis.getSelectExpressions()
    );

    return new JoinNode(
        new PlanNodeId("Join"),
        analysis.getSelectExpressions(),
        joinInfo.get().getType(),
        leftSourceNode,
        rightSourceNode,
        joinInfo.get().getLeftJoinField(),
        joinInfo.get().getRightJoinField(),
        joinInfo.get().getWithinExpression()
    );
  }

  private DataSourceNode buildNonJoinNode(final List<AliasedDataSource> sources) {
    if (sources.size() != 1) {
      throw new IllegalStateException("Expected only 1 source, got: " + sources.size());
    }

    final AliasedDataSource dataSource = analysis.getFromDataSources().get(0);
    return new DataSourceNode(
        new PlanNodeId("KaypherTopic"),
        dataSource.getDataSource(),
        dataSource.getAlias(),
        analysis.getSelectExpressions()
    );
  }

  private Optional<ColumnName> getSelectAliasMatching(
      final BiFunction<Expression, ColumnName, Boolean> matcher,
      final PlanNode sourcePlanNode
  ) {
    for (int i = 0; i < sourcePlanNode.getSelectExpressions().size(); i++) {
      final SelectExpression select = sourcePlanNode.getSelectExpressions().get(i);

      if (matcher.apply(select.getExpression(), select.getAlias())) {
        return Optional.of(select.getAlias());
      }
    }

    return Optional.empty();
  }

  private LogicalSchema buildProjectionSchema(final PlanNode sourcePlanNode) {
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        sourcePlanNode.getSchema(),
        functionRegistry
    );

    final Builder builder = LogicalSchema.builder();

    final List<Column> keyColumns = sourcePlanNode.getSchema().isAliased()
        ? sourcePlanNode.getSchema().withoutAlias().key()
        : sourcePlanNode.getSchema().key();

    builder.keyColumns(keyColumns);

    for (int i = 0; i < sourcePlanNode.getSelectExpressions().size(); i++) {
      final SelectExpression select = sourcePlanNode.getSelectExpressions().get(i);

      final SqlType expressionType = expressionTypeManager
          .getExpressionSqlType(select.getExpression());

      builder.valueColumn(select.getAlias(), expressionType);
    }

    return builder.build();
  }
}
