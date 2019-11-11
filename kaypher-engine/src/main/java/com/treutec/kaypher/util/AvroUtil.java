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
package com.treutec.kaypher.util;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import com.treutec.kaypher.execution.ddl.commands.CreateSourceCommand;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.schema.kaypher.PhysicalSchema;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.avro.AvroSchemas;
import java.io.IOException;
import org.apache.http.HttpStatus;

public final class AvroUtil {

  private AvroUtil() {
  }

  public static void throwOnInvalidSchemaEvolution(
      final String statementText,
      final CreateSourceCommand ddl,
      final SchemaRegistryClient schemaRegistryClient,
      final KaypherConfig kaypherConfig
  ) {
    final KaypherTopic topic = ddl.getTopic();
    final FormatInfo format = topic.getValueFormat().getFormatInfo();
    if (format.getFormat() != Format.AVRO) {
      return;
    }

    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        ddl.getSchema(),
        ddl.getSerdeOptions()
    );
    final org.apache.avro.Schema avroSchema = AvroSchemas.getAvroSchema(
        physicalSchema.valueSchema(),
        format.getAvroFullSchemaName().orElse(KaypherConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME),
        kaypherConfig
    );

    final String topicName = topic.getKafkaTopicName();

    if (!isValidAvroSchemaForTopic(topicName, avroSchema, schemaRegistryClient)) {
      throw new KaypherStatementException(String.format(
          "Cannot register avro schema for %s as the schema is incompatible with the current "
              + "schema version registered for the topic.%n"
              + "KAYPHER schema: %s%n"
              + "Registered schema: %s",
          topicName,
          avroSchema,
          getRegisteredSchema(topicName, schemaRegistryClient)
      ), statementText);
    }
  }

  private static String getRegisteredSchema(
      final String topicName,
      final SchemaRegistryClient schemaRegistryClient) {
    try {
      return schemaRegistryClient
          .getLatestSchemaMetadata(topicName + KaypherConstants.SCHEMA_REGISTRY_VALUE_SUFFIX)
          .getSchema();
    } catch (Exception e) {
      return "Could not get registered schema due to exception: " + e.getMessage();
    }
  }

  private static boolean isValidAvroSchemaForTopic(
      final String topicName,
      final org.apache.avro.Schema avroSchema,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    try {
      return schemaRegistryClient.testCompatibility(
          topicName + KaypherConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, avroSchema);
    } catch (final IOException e) {
      throw new KaypherException(String.format(
          "Could not check Schema compatibility: %s", e.getMessage()
      ));
    } catch (final RestClientException e) {
      if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
        // Assume the subject is unknown.
        // See https://github.com/confluentinc/schema-registry/issues/951
        return true;
      }

      String errorMessage = e.getMessage();
      if (e.getStatus() == HttpStatus.SC_UNAUTHORIZED || e.getStatus() == HttpStatus.SC_FORBIDDEN) {
        errorMessage = String.format(
            "Not authorized to access Schema Registry subject: [%s]",
            topicName + KaypherConstants.SCHEMA_REGISTRY_VALUE_SUFFIX
        );
      }

      throw new KaypherException(String.format(
          "Could not connect to Schema Registry service: %s", errorMessage
      ));
    }
  }
}
