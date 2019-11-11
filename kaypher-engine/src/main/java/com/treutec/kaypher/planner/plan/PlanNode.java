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

import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.structured.SchemaKStream;
import java.util.List;


public abstract class PlanNode {

  private final PlanNodeId id;
  private final DataSourceType nodeOutputType;

  protected PlanNode(final PlanNodeId id, final DataSourceType nodeOutputType) {
    requireNonNull(id, "id is null");
    requireNonNull(nodeOutputType, "nodeOutputType is null");
    this.id = id;
    this.nodeOutputType = nodeOutputType;
  }

  public PlanNodeId getId() {
    return id;
  }

  public DataSourceType getNodeOutputType() {
    return nodeOutputType;
  }

  public abstract LogicalSchema getSchema();

  public abstract KeyField getKeyField();

  public abstract List<PlanNode> getSources();
  
  public abstract List<SelectExpression> getSelectExpressions();

  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitPlan(this, context);
  }

  public DataSourceNode getTheSourceNode() {
    if (this instanceof DataSourceNode) {
      return (DataSourceNode) this;
    } else if (this.getSources() != null && !this.getSources().isEmpty()) {
      return this.getSources().get(0).getTheSourceNode();
    }
    return null;
  }

  protected abstract int getPartitions(KafkaTopicClient kafkaTopicClient);

  public abstract SchemaKStream<?> buildStream(KaypherQueryBuilder builder);
}
