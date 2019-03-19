/*
 * Copyright 2019 Koneksys
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

package com.koneksys.kaypher.metastore.model;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import com.koneksys.kaypher.metastore.SerdeFactory;
import com.koneksys.kaypher.schema.kaypher.kaypherSchema;
import com.koneksys.kaypher.serde.DataSource;
import com.koneksys.kaypher.serde.kaypherTopicSerDe;
import com.koneksys.kaypher.util.timestamp.TimestampExtractionPolicy;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

@Immutable
public abstract class StructuredDataSource<K> implements DataSource {

  private final String dataSourceName;
  private final DataSourceType dataSourceType;
  private final kaypherSchema schema;
  private final Optional<Field> keyField;
  private final TimestampExtractionPolicy timestampExtractionPolicy;
  private final SerdeFactory<K> keySerde;
  private final kaypherTopic kaypherTopic;
  private final String sqlExpression;

  public StructuredDataSource(
      final String sqlExpression,
      final String dataSourceName,
      final Schema schema,
      final Optional<Field> keyField,
      final TimestampExtractionPolicy tsExtractionPolicy,
      final DataSourceType dataSourceType,
      final kaypherTopic kaypherTopic,
      final SerdeFactory<K> keySerde
  ) {
    this.sqlExpression = requireNonNull(sqlExpression, "sqlExpression");
    this.dataSourceName = requireNonNull(dataSourceName, "dataSourceName");
    this.schema = kaypherSchema.of(schema);
    this.keyField = requireNonNull(keyField, "keyField");
    this.timestampExtractionPolicy = requireNonNull(tsExtractionPolicy, "tsExtractionPolicy");
    this.dataSourceType = requireNonNull(dataSourceType, "dataSourceType");
    this.kaypherTopic = requireNonNull(kaypherTopic, "kaypherTopic");
    this.keySerde = requireNonNull(keySerde, "keySerde");
  }

  @Override
  public String getName() {
    return this.dataSourceName;
  }

  @Override
  public DataSourceType getDataSourceType() {
    return this.dataSourceType;
  }

  public Schema getSchema() {
    return schema.getSchema();
  }

  public Optional<Field> getKeyField() {
    return keyField;
  }

  public kaypherTopic getkaypherTopic() {
    return kaypherTopic;
  }

  public SerdeFactory<K> getKeySerdeFactory() {
    return keySerde;
  }

  public kaypherTopicSerDe getkaypherTopicSerde() {
    return kaypherTopic.getkaypherTopicSerDe();
  }

  public boolean isSerdeFormat(final DataSource.DataSourceSerDe format) {
    return getkaypherTopicSerde() != null && getkaypherTopicSerde().getSerDe() == format;
  }

  public TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return timestampExtractionPolicy;
  }

  public String getkaypherTopicName() {
    return kaypherTopic.getkaypherTopicName();
  }

  public String getKafkaTopicName() {
    return kaypherTopic.getKafkaTopicName();
  }

  public String getSqlExpression() {
    return sqlExpression;
  }

  public abstract StructuredDataSource<?> cloneWithTimeKeyColumns();
}
