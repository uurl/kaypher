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
package com.treutec.kaypher.schema.kaypher.inference;

import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;

/**
 * Supplier of schemas for topics
 */
public interface TopicSchemaSupplier {

  /**
   * Get the value schema for the supplied {@code topicName}.
   *
   * @param topicName the name of the topic.
   * @param schemaId  optional schema id to retrieve.
   * @return the schema and id or an error message should the schema not be present or compatible.
   * @throws RuntimeException on communication issues with remote services.
   */
  SchemaResult getValueSchema(String topicName, Optional<Integer> schemaId);

  final class SchemaAndId {

    final int id;
    final Schema schema;

    private SchemaAndId(final Schema schema, final int id) {
      this.id = id;
      this.schema = Objects.requireNonNull(schema, "schema");
    }

    static SchemaAndId schemaAndId(final Schema schema, final int id) {
      return new SchemaAndId(schema, id);
    }
  }

  final class SchemaResult {

    final Optional<SchemaAndId> schemaAndId;
    final Optional<Exception> failureReason;

    private SchemaResult(
        final Optional<SchemaAndId> schemaAndId,
        final Optional<Exception> failureReason
    ) {
      this.schemaAndId = Objects.requireNonNull(schemaAndId, "schemaAndId");
      this.failureReason = Objects.requireNonNull(failureReason, "failureReason");
    }

    static SchemaResult success(final SchemaAndId schemaAndId) {
      return new SchemaResult(Optional.of(schemaAndId), Optional.empty());
    }

    static SchemaResult failure(final Exception cause) {
      return new SchemaResult(Optional.empty(), Optional.of(cause));
    }

    public Optional<SchemaAndId> getSchemaAndId() {
      return schemaAndId;
    }

    public Optional<Exception> getFailureReason() {
      return failureReason;
    }
  }
}
