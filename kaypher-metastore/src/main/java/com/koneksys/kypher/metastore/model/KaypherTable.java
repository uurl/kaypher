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

import com.google.errorprone.annotations.Immutable;
import com.koneksys.kaypher.metastore.SerdeFactory;
import com.koneksys.kaypher.util.SchemaUtil;
import com.koneksys.kaypher.util.timestamp.TimestampExtractionPolicy;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.WindowedSerdes;

@Immutable
public class KaypherTable<K> extends StructuredDataSource<K> {

  public kaypherTable(
      final String sqlExpression,
      final String datasourceName,
      final Schema schema,
      final Optional<Field> keyField,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final kaypherTopic kaypherTopic,
      final SerdeFactory<K> keySerde
  ) {
    super(
        sqlExpression,
        datasourceName,
        schema,
        keyField,
        timestampExtractionPolicy,
        DataSourceType.KTABLE,
        kaypherTopic,
        keySerde
    );
  }

  public boolean isWindowed() {
    final Serde<K> keySerde = getKeySerdeFactory().create();
    return keySerde instanceof WindowedSerdes.SessionWindowedSerde
        || keySerde instanceof WindowedSerdes.TimeWindowedSerde;
  }

  @Override
  public kaypherTable<K> cloneWithTimeKeyColumns() {
    final Schema newSchema = SchemaUtil.addImplicitRowTimeRowKeyToSchema(getSchema());
    return new kaypherTable<>(
        getSqlExpression(),
        getName(),
        newSchema,
        getKeyField(),
        getTimestampExtractionPolicy(),
        getkaypherTopic(),
        getKeySerdeFactory()
    );
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " name:" + getName();
  }
}
