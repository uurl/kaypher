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

package com.treutec.kaypher.serde.avro;

import com.google.errorprone.annotations.Immutable;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.KaypherSerdeFactory;
import io.confluent.ksql.serde.connect.KaypherConnectDeserializer;
import io.confluent.ksql.serde.connect.KaypherConnectSerializer;
import io.confluent.ksql.serde.tls.ThreadLocalDeserializer;
import io.confluent.ksql.serde.tls.ThreadLocalSerializer;
import io.confluent.ksql.util.KaypherConfig;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

@Immutable
public class KaypherAvroSerdeFactory implements KaypherSerdeFactory {

  private final String fullSchemaName;

  public KaypherAvroSerdeFactory(final String fullSchemaName) {
    this.fullSchemaName = Objects.requireNonNull(fullSchemaName, "fullSchemaName").trim();
    if (this.fullSchemaName.isEmpty()) {
      throw new IllegalArgumentException("the schema name cannot be empty");
    }
  }

  @Override
  public void validate(final PersistenceSchema schema) {
    // Supports all types
  }

  @Override
  public Serde<Object> createSerde(
      final PersistenceSchema schema,
      final KaypherConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final Supplier<Serializer<Object>> serializerSupplier = () -> createConnectSerializer(
        schema,
        ksqlConfig,
        schemaRegistryClientFactory
    );

    final Supplier<Deserializer<Object>> deserializerSupplier = () -> createConnectDeserializer(
        schema,
        ksqlConfig,
        schemaRegistryClientFactory);

    // Sanity check:
    serializerSupplier.get();
    deserializerSupplier.get();

    return Serdes.serdeFrom(
        new ThreadLocalSerializer<>(serializerSupplier),
        new ThreadLocalDeserializer<>(deserializerSupplier)
    );
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KaypherAvroSerdeFactory that = (KaypherAvroSerdeFactory) o;
    return Objects.equals(fullSchemaName, that.fullSchemaName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullSchemaName);
  }

  private KaypherConnectSerializer createConnectSerializer(
      final PersistenceSchema schema,
      final KaypherConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final AvroDataTranslator translator = createAvroTranslator(schema, ksqlConfig);

    final AvroConverter avroConverter =
        getAvroConverter(schemaRegistryClientFactory.get(), ksqlConfig);

    return new KaypherConnectSerializer(
        translator.getAvroCompatibleSchema(),
        translator,
        avroConverter
    );
  }

  private KaypherConnectDeserializer createConnectDeserializer(
      final PersistenceSchema schema,
      final KaypherConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final AvroDataTranslator translator = createAvroTranslator(schema, ksqlConfig);

    final AvroConverter avroConverter =
        getAvroConverter(schemaRegistryClientFactory.get(), ksqlConfig);

    return new KaypherConnectDeserializer(avroConverter, translator);
  }

  private AvroDataTranslator createAvroTranslator(
      final PersistenceSchema schema,
      final KaypherConfig ksqlConfig
  ) {
    final boolean useNamedMaps = ksqlConfig.getBoolean(KaypherConfig.KAYPHER_USE_NAMED_AVRO_MAPS);

    return new AvroDataTranslator(schema.serializedSchema(), fullSchemaName, useNamedMaps);
  }

  private static AvroConverter getAvroConverter(
      final SchemaRegistryClient schemaRegistryClient,
      final KaypherConfig ksqlConfig
  ) {
    final AvroConverter avroConverter = new AvroConverter(schemaRegistryClient);

    final Map<String, Object> avroConfig = ksqlConfig
        .originalsWithPrefix(KaypherConfig.KAYPHER_SCHEMA_REGISTRY_PREFIX);

    avroConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        ksqlConfig.getString(KaypherConfig.SCHEMA_REGISTRY_URL_PROPERTY));

    avroConfig.put(AvroDataConfig.CONNECT_META_DATA_CONFIG, false);

    avroConverter.configure(avroConfig, false);
    return avroConverter;
  }
}
