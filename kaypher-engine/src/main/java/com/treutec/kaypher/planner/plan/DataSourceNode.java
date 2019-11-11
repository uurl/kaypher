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
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.context.QueryContext.Stacker;
import com.treutec.kaypher.execution.plan.LogicalSchemaWithMetaAndKeyFields;
import com.treutec.kaypher.execution.plan.SelectExpression;
import com.treutec.kaypher.execution.plan.StreamSource;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.structured.SchemaKStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

@Immutable
public class DataSourceNode extends PlanNode {

  private static final String SOURCE_OP_NAME = "source";
  private static final String REDUCE_OP_NAME = "reduce";

  private final DataSource<?> dataSource;
  private final SourceName alias;
  private final LogicalSchemaWithMetaAndKeyFields schema;
  private final KeyField keyField;
  private final SchemaKStreamFactory schemaKStreamFactory;
  private final List<SelectExpression> selectExpressions;

  public DataSourceNode(
      final PlanNodeId id,
      final DataSource<?> dataSource,
      final SourceName alias,
      final List<SelectExpression> selectExpressions
  ) {
    this(id, dataSource, alias, selectExpressions, SchemaKStream::forSource);
  }

  DataSourceNode(
      final PlanNodeId id,
      final DataSource<?> dataSource,
      final SourceName alias,
      final List<SelectExpression> selectExpressions,
      final SchemaKStreamFactory schemaKStreamFactory
  ) {
    super(id, dataSource.getDataSourceType());
    this.dataSource = requireNonNull(dataSource, "dataSource");
    this.alias = requireNonNull(alias, "alias");
    this.selectExpressions =
        ImmutableList.copyOf(requireNonNull(selectExpressions, "selectExpressions"));

    // DataSourceNode copies implicit and key fields into the value schema
    // It users a KS valueMapper to add the key fields
    // and a KS transformValues to add the implicit fields
    this.schema = StreamSource.getSchemaWithMetaAndKeyFields(alias, dataSource.getSchema());

    this.keyField = dataSource.getKeyField()
        .withAlias(alias)
        .validateKeyExistsIn(schema.getSchema());

    this.schemaKStreamFactory = requireNonNull(schemaKStreamFactory, "schemaKStreamFactory");
  }

  @Override
  public LogicalSchema getSchema() {
    return schema.getSchema();
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  public DataSource<?> getDataSource() {
    return dataSource;
  }

  SourceName getAlias() {
    return alias;
  }

  public DataSourceType getDataSourceType() {
    return dataSource.getDataSourceType();
  }

  @Override
  public int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    final String topicName = dataSource.getKaypherTopic().getKafkaTopicName();

    return kafkaTopicClient.describeTopic(topicName)
        .partitions()
        .size();
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of();
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitDataSourceNode(this, context);
  }

  @Override
  public SchemaKStream<?> buildStream(final KaypherQueryBuilder builder) {
    final Stacker contextStacker = builder.buildNodeContext(getId().toString());
    final SchemaKStream<?> schemaKStream = schemaKStreamFactory.create(
        builder,
        dataSource,
        schema,
        contextStacker.push(SOURCE_OP_NAME),
        timestampIndex(),
        getAutoOffsetReset(builder.getKaypherConfig().getKaypherStreamConfigProps()),
        keyField
    );
    if (getDataSourceType() == DataSourceType.KSTREAM) {
      return schemaKStream;
    }
    final Stacker reduceContextStacker = contextStacker.push(REDUCE_OP_NAME);
    return schemaKStream.toTable(
        dataSource.getKaypherTopic().getKeyFormat(),
        dataSource.getKaypherTopic().getValueFormat(),
        reduceContextStacker
    );
  }

  interface SchemaKStreamFactory {
    SchemaKStream<?> create(
        KaypherQueryBuilder builder,
        DataSource<?> dataSource,
        LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
        QueryContext.Stacker contextStacker,
        int timestampIndex,
        Optional<AutoOffsetReset> offsetReset,
        KeyField keyField
    );
  }

  private int timestampIndex() {
    final LogicalSchema originalSchema = dataSource.getSchema();
    final ColumnRef timestampField = dataSource.getTimestampExtractionPolicy().timestampField();
    if (timestampField == null) {
      return -1;
    }

    return originalSchema.valueColumnIndex(timestampField)
        .orElseThrow(IllegalStateException::new);
  }

  private static Optional<Topology.AutoOffsetReset> getAutoOffsetReset(
      final Map<String, Object> props) {
    final Object offestReset = props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    if (offestReset == null) {
      return Optional.empty();
    }

    try {
      return Optional.of(AutoOffsetReset.valueOf(offestReset.toString().toUpperCase()));
    } catch (final Exception e) {
      throw new ConfigException(
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          offestReset,
          "Unknown value"
      );
    }
  }
}
