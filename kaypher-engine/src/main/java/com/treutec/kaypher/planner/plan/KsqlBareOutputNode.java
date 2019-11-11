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

import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.query.id.QueryIdGenerator;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.structured.SchemaKStream;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicy;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KaypherBareOutputNode extends OutputNode {

  private final KeyField keyField;

  public KaypherBareOutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final OptionalInt limit,
      final TimestampExtractionPolicy extractionPolicy
  ) {
    super(id, source, schema, limit, extractionPolicy);
    this.keyField = KeyField.of(source.getKeyField().ref())
        .validateKeyExistsIn(schema);
  }

  @Override
  public QueryId getQueryId(final QueryIdGenerator queryIdGenerator) {
    return new QueryId(String.valueOf(Math.abs(ThreadLocalRandom.current().nextLong())));
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public SchemaKStream<?> buildStream(final KaypherQueryBuilder builder) {
    return getSource().buildStream(builder);
  }
}
