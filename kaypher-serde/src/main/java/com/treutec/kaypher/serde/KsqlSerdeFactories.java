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

package com.treutec.kaypher.serde;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.avro.KaypherAvroSerdeFactory;
import io.confluent.ksql.serde.delimited.KaypherDelimitedSerdeFactory;
import io.confluent.ksql.serde.json.KaypherJsonSerdeFactory;
import io.confluent.ksql.serde.kafka.KafkaSerdeFactory;
import io.confluent.ksql.util.KaypherConfig;
import io.confluent.ksql.util.KaypherConstants;
import io.confluent.ksql.util.KaypherException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;

final class KaypherSerdeFactories implements SerdeFactories {

  private final Function<FormatInfo, KaypherSerdeFactory> factoryMethod;

  KaypherSerdeFactories() {
    this(KaypherSerdeFactories::create);
  }

  @VisibleForTesting
  KaypherSerdeFactories(final Function<FormatInfo, KaypherSerdeFactory> factoryMethod) {
    this.factoryMethod = Objects.requireNonNull(factoryMethod, "factoryMethod");
  }

  @Override
  public <K> Serde<K> create(
      final FormatInfo format,
      final PersistenceSchema schema,
      final KaypherConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final Class<K> type
  ) {
    final KaypherSerdeFactory ksqlSerdeFactory = factoryMethod.apply(format);

    ksqlSerdeFactory.validate(schema);

    return ksqlSerdeFactory.createSerde(schema, ksqlConfig, schemaRegistryClientFactory, type);
  }

  @VisibleForTesting
  static KaypherSerdeFactory create(final FormatInfo format) {
    switch (format.getFormat()) {
      case AVRO:
        final String schemaFullName = format.getAvroFullSchemaName()
            .orElse(KaypherConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME);

        return new KaypherAvroSerdeFactory(schemaFullName);

      case JSON:
        return new KaypherJsonSerdeFactory();

      case DELIMITED:
        return new KaypherDelimitedSerdeFactory(format.getDelimiter());

      case KAFKA:
        return new KafkaSerdeFactory();

      default:
        throw new KaypherException(
            String.format("Unsupported format: %s", format.getFormat()));
    }
  }
}
