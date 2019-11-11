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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.query.id.QueryIdGenerator;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import javax.annotation.concurrent.Immutable;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Immutable
public abstract class OutputNode
    extends PlanNode {

  private final PlanNode source;
  private final LogicalSchema schema;
  private final OptionalInt limit;
  private final TimestampExtractionPolicy timestampExtractionPolicy;

  protected OutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final OptionalInt limit,
      final TimestampExtractionPolicy timestampExtractionPolicy
  ) {
    super(id, source.getNodeOutputType());

    this.source = requireNonNull(source, "source");
    this.schema = requireNonNull(schema, "schema");
    this.limit = requireNonNull(limit, "limit");
    this.timestampExtractionPolicy =
        requireNonNull(timestampExtractionPolicy, "timestampExtractionPolicy");
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public OptionalInt getLimit() {
    return limit;
  }

  public PlanNode getSource() {
    return source;
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return Collections.emptyList();
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitOutput(this, context);
  }

  public TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return timestampExtractionPolicy;
  }

  public abstract QueryId getQueryId(QueryIdGenerator queryIdGenerator);
}
