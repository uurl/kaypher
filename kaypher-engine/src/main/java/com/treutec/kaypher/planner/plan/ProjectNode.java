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
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.structured.SchemaKStream;
import com.treutec.kaypher.util.KaypherException;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ProjectNode extends PlanNode {

  private final PlanNode source;
  private final LogicalSchema schema;
  private final List<SelectExpression> projectExpressions;
  private final KeyField keyField;

  public ProjectNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final Optional<ColumnRef> keyFieldName
  ) {
    super(id, source.getNodeOutputType());

    this.source = requireNonNull(source, "source");
    this.schema = requireNonNull(schema, "schema");
    this.projectExpressions = ImmutableList.copyOf(source.getSelectExpressions());
    this.keyField = KeyField.of(requireNonNull(keyFieldName, "keyFieldName"))
        .validateKeyExistsIn(schema);

    if (schema.value().size() != projectExpressions.size()) {
      throw new KaypherException("Error in projection. Schema fields and expression list are not "
          + "compatible.");
    }

    validate();
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public PlanNode getSource() {
    return source;
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return projectExpressions;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitProject(this, context);
  }

  @Override
  public SchemaKStream<?> buildStream(final KaypherQueryBuilder builder) {
    return getSource().buildStream(builder)
        .select(
            getSelectExpressions(),
            builder.buildNodeContext(getId().toString()),
            builder
        );
  }

  private void validate() {
    for (int i = 0; i < projectExpressions.size(); i++) {
      final Column column = schema.value().get(i);
      final SelectExpression selectExpression = projectExpressions.get(i);

      if (!column.name().equals(selectExpression.getAlias())) {
        throw new IllegalArgumentException("Mismatch between schema and selects");
      }
    }
  }
}
