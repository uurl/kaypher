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

import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicy;
import java.util.Set;

public interface DataSource<K> {

  enum DataSourceType {
    KSTREAM("STREAM"),
    KTABLE("TABLE");

    private final String kaypherType;

    DataSourceType(final String kaypherType) {
      this.kaypherType = kaypherType;
    }

    public String getKaypherType() {
      return kaypherType;
    }
  }

  /**
   * @return the name of the data source.
   */
  SourceName getName();

  /**
   * @return the type of the data source.
   */
  DataSourceType getDataSourceType();

  /**
   * Get the logical schema of the source.
   *
   * <p>The logical schema is the schema used by KAYPHER when building queries.
   *
   * @return the physical schema.
   */
  LogicalSchema getSchema();

  /**
   * Get the physical serde options of the source.
   *
   * <p>These options can be combined with the logical schema to build the {@code PhysicalSchema} of
   * the source.
   *
   * @return the source's serde options.
   */
  Set<SerdeOption> getSerdeOptions();

  /**
   * @return the key field of the source.
   */
  KeyField getKeyField();

  /**
   * @return the topic backing the source.
   */
  KaypherTopic getKaypherTopic();

  /**
   * The timestamp extraction policy of the source.
   *
   * <p>This is controlled by the
   * {@link com.treutec.kaypher.properties.with.CommonCreateConfigs#TIMESTAMP_NAME_PROPERTY}
   * and {@link com.treutec.kaypher.properties.with.CommonCreateConfigs#TIMESTAMP_FORMAT_PROPERTY}
   * properties set in the WITH clause.
   *
   * @return the timestamp extraction policy of the source.
   */
  TimestampExtractionPolicy getTimestampExtractionPolicy();

  /**
   * @return the name of the KAFKA topic backing this source.
   */
  String getKafkaTopicName();

  /**
   * @return the SQL statement used to create this source.
   */
  String getSqlExpression();
}
