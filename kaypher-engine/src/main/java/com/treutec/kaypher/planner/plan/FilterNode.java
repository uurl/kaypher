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
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.structured.SchemaKStream;
import java.util.List;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class FilterNode extends PlanNode {

  private final PlanNode source;
  private final Expression predicate;
  private final List<SelectExpression> selectExpressions;

  public FilterNode(
      final PlanNodeId id,
      final PlanNode source,
      final Expression predicate
  ) {
    super(id, source.getNodeOutputType());

    this.source = Objects.requireNonNull(source, "source");
    this.predicate = Objects.requireNonNull(predicate, "predicate");
    this.selectExpressions = ImmutableList.copyOf(source.getSelectExpressions());
  }

  public Expression getPredicate() {
    return predicate;
  }

  @Override
  public LogicalSchema getSchema() {
    return source.getSchema();
  }

  @Override
  public KeyField getKeyField() {
    return source.getKeyField();
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
  }

  public PlanNode getSource() {
    return source;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitFilter(this, context);
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public SchemaKStream<?> buildStream(final KaypherQueryBuilder builder) {
    return getSource().buildStream(builder)
        .filter(
            getPredicate(),
            builder.buildNodeContext(getId().toString())
        );
  }
}
