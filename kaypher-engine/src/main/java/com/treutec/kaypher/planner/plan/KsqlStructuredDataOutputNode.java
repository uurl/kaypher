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

import static com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableSet;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.query.id.QueryIdGenerator;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.structured.SchemaKStream;
import com.treutec.kaypher.structured.SchemaKTable;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicy;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

public class KaypherStructuredDataOutputNode extends OutputNode {

  private final KaypherTopic kaypherTopic;
  private final KeyField keyField;
  private final Optional<ColumnRef> partitionByField;
  private final boolean doCreateInto;
  private final Set<SerdeOption> serdeOptions;
  private final SourceName intoSourceName;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public KaypherStructuredDataOutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final KeyField keyField,
      final KaypherTopic kaypherTopic,
      final Optional<ColumnRef> partitionByField,
      final OptionalInt limit,
      final boolean doCreateInto,
      final Set<SerdeOption> serdeOptions,
      final SourceName intoSourceName) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    super(
        id,
        source,
        // KAYPHER internally copies the implicit and key fields into the value schema.
        // This is done by DataSourceNode
        // Hence, they must be removed again here if they are still in the sink schema.
        // This leads to strange behaviour, but changing it is a breaking change.
        schema.withoutMetaAndKeyColsInValue(),
        limit,
        timestampExtractionPolicy
    );

    this.serdeOptions = ImmutableSet.copyOf(requireNonNull(serdeOptions, "serdeOptions"));
    this.keyField = requireNonNull(keyField, "keyField")
        .validateKeyExistsIn(schema);
    this.kaypherTopic = requireNonNull(kaypherTopic, "kaypherTopic");
    this.partitionByField = Objects.requireNonNull(partitionByField, "partitionByField");
    this.doCreateInto = doCreateInto;
    this.intoSourceName = requireNonNull(intoSourceName, "intoSourceName");

    validatePartitionByField();
  }

  public boolean isDoCreateInto() {
    return doCreateInto;
  }

  public KaypherTopic getKaypherTopic() {
    return kaypherTopic;
  }

  public Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  public SourceName getIntoSourceName() {
    return intoSourceName;
  }

  @Override
  public QueryId getQueryId(final QueryIdGenerator queryIdGenerator) {
    final String base = queryIdGenerator.getNext();
    if (!doCreateInto) {
      return new QueryId("InsertQuery_" + base);
    }
    if (getNodeOutputType().equals(DataSourceType.KTABLE)) {
      return new QueryId("CTAS_" + getId().toString() + "_" + base);
    }
    return new QueryId("CSAS_" + getId().toString() + "_" + base);
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public SchemaKStream<?> buildStream(final KaypherQueryBuilder builder) {
    final PlanNode source = getSource();
    final SchemaKStream schemaKStream = source.buildStream(builder);

    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId().toString());

    final SchemaKStream<?> result = createOutputStream(
        schemaKStream,
        contextStacker
    );

    return result.into(
        getKaypherTopic().getKafkaTopicName(),
        getSchema(),
        getKaypherTopic().getValueFormat(),
        serdeOptions,
        contextStacker
    );
  }

  private SchemaKStream<?> createOutputStream(
      final SchemaKStream schemaKStream,
      final QueryContext.Stacker contextStacker
  ) {
    if (schemaKStream instanceof SchemaKTable) {
      return schemaKStream;
    }

    if (!partitionByField.isPresent()) {
      return schemaKStream;
    }

    return schemaKStream.selectKey(partitionByField.get(), false, contextStacker);
  }

  private void validatePartitionByField() {
    if (!partitionByField.isPresent()) {
      return;
    }

    final ColumnRef fieldName = partitionByField.get();

    if (getSchema().isMetaColumn(fieldName.name()) || getSchema().isKeyColumn(fieldName.name())) {
      return;
    }

    if (!keyField.ref().equals(Optional.of(fieldName))) {
      throw new IllegalArgumentException("keyField must match partition by field");
    }
  }
}