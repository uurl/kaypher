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
package com.treutec.kaypher.metastore.model;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.util.SchemaUtil;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicy;
import java.util.Set;

@Immutable
abstract class StructuredDataSource<K> implements DataSource<K> {

  private final SourceName dataSourceName;
  private final DataSourceType dataSourceType;
  private final LogicalSchema schema;
  private final KeyField keyField;
  private final TimestampExtractionPolicy timestampExtractionPolicy;
  private final KaypherTopic kaypherTopic;
  private final String sqlExpression;
  private final ImmutableSet<SerdeOption> serdeOptions;

  StructuredDataSource(
      final String sqlExpression,
      final SourceName dataSourceName,
      final LogicalSchema schema,
      final Set<SerdeOption> serdeOptions,
      final KeyField keyField,
      final TimestampExtractionPolicy tsExtractionPolicy,
      final DataSourceType dataSourceType,
      final KaypherTopic kaypherTopic
  ) {
    this.sqlExpression = requireNonNull(sqlExpression, "sqlExpression");
    this.dataSourceName = requireNonNull(dataSourceName, "dataSourceName");
    this.schema = requireNonNull(schema, "schema");
    this.keyField = requireNonNull(keyField, "keyField")
        .validateKeyExistsIn(schema);
    this.timestampExtractionPolicy = requireNonNull(tsExtractionPolicy, "tsExtractionPolicy");
    this.dataSourceType = requireNonNull(dataSourceType, "dataSourceType");
    this.kaypherTopic = requireNonNull(kaypherTopic, "kaypherTopic");
    this.serdeOptions = ImmutableSet.copyOf(requireNonNull(serdeOptions, "serdeOptions"));

    if (schema.findValueColumn(ColumnRef.withoutSource(SchemaUtil.ROWKEY_NAME)).isPresent()
        || schema.findValueColumn(ColumnRef.withoutSource(SchemaUtil.ROWTIME_NAME)).isPresent()) {
      throw new IllegalArgumentException("Schema contains implicit columns in value schema");
    }
  }

  @Override
  public SourceName getName() {
    return dataSourceName;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return this.dataSourceType;
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public KaypherTopic getKaypherTopic() {
    return kaypherTopic;
  }

  @Override
  public TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return timestampExtractionPolicy;
  }

  @Override
  public String getKafkaTopicName() {
    return kaypherTopic.getKafkaTopicName();
  }

  @Override
  public String getSqlExpression() {
    return sqlExpression;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " name:" + getName();
  }
}
