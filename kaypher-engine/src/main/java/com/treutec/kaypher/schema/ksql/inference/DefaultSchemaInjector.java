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

import com.google.common.collect.Iterables;
import com.treutec.kaypher.metastore.TypeRegistry;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.SchemaParser;
import com.treutec.kaypher.parser.SqlFormatter;
import com.treutec.kaypher.parser.properties.with.CreateSourceProperties;
import com.treutec.kaypher.parser.tree.CreateSource;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.parser.tree.TableElements;
import com.treutec.kaypher.schema.connect.SqlSchemaFormatter;
import com.treutec.kaypher.schema.connect.SqlSchemaFormatter.Option;
import com.treutec.kaypher.schema.kaypher.SchemaConverters;
import com.treutec.kaypher.schema.kaypher.inference.TopicSchemaSupplier.SchemaAndId;
import com.treutec.kaypher.schema.kaypher.inference.TopicSchemaSupplier.SchemaResult;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.statement.Injector;
import com.treutec.kaypher.util.IdentifierUtil;
import com.treutec.kaypher.util.KaypherStatementException;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;

/**
 * An injector which injects the schema into the supplied {@code statement}.
 *
 * <p>The schema is only injected if:
 * <ul>
 * <li>The statement is a CT/CS.</li>
 * <li>The statement does not defined a schema.</li>
 * <li>The format of the statement supports schema inference.</li>
 * </ul>
 *
 * <p>If any of the above are not true then the {@code statement} is returned unchanged.
 */
public class DefaultSchemaInjector implements Injector {

  private static final SqlSchemaFormatter FORMATTER = new SqlSchemaFormatter(
      w -> !IdentifierUtil.isValid(w), Option.AS_COLUMN_LIST);

  private final TopicSchemaSupplier schemaSupplier;

  public DefaultSchemaInjector(final TopicSchemaSupplier schemaSupplier) {
    this.schemaSupplier = Objects.requireNonNull(schemaSupplier, "schemaSupplier");
  }


  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement
  ) {
    if (!(statement.getStatement() instanceof CreateSource)) {
      return statement;
    }

    final ConfiguredStatement<CreateSource> createStatement =
        (ConfiguredStatement<CreateSource>) statement;

    return (ConfiguredStatement<T>) forCreateStatement(createStatement).orElse(createStatement);
  }

  private Optional<ConfiguredStatement<CreateSource>> forCreateStatement(
      final ConfiguredStatement<CreateSource> statement
  ) {
    if (hasElements(statement)
        || statement.getStatement().getProperties().getValueFormat() != Format.AVRO) {
      return Optional.empty();
    }

    final SchemaAndId valueSchema = getValueSchema(statement);
    final CreateSource withSchema = addSchemaFields(statement, valueSchema);
    final PreparedStatement<CreateSource> prepared =
        buildPreparedStatement(withSchema);
    return Optional.of(ConfiguredStatement.of(
        prepared, statement.getOverrides(), statement.getConfig()));
  }

  private SchemaAndId getValueSchema(
      final ConfiguredStatement<CreateSource> statement
  ) {
    final String topicName = statement.getStatement().getProperties().getKafkaTopic();

    final SchemaResult result = statement.getStatement().getProperties().getAvroSchemaId()
        .map(id -> schemaSupplier.getValueSchema(topicName, Optional.of(id)))
        .orElseGet(() -> schemaSupplier.getValueSchema(topicName, Optional.empty()));

    if (result.failureReason.isPresent()) {
      final Exception cause = result.failureReason.get();
      throw new KaypherStatementException(
          cause.getMessage(),
          statement.getStatementText(),
          cause);
    }

    return result.schemaAndId.get();
  }

  private static boolean hasElements(
      final ConfiguredStatement<CreateSource> statement
  ) {
    return !Iterables.isEmpty(statement.getStatement().getElements());
  }

  private static CreateSource addSchemaFields(
      final ConfiguredStatement<CreateSource> preparedStatement,
      final SchemaAndId schema
  ) {
    final TableElements elements = buildElements(schema.schema, preparedStatement);

    final CreateSource statement = preparedStatement.getStatement();
    final CreateSourceProperties properties = statement.getProperties();

    if (properties.getAvroSchemaId().isPresent()) {
      return statement.copyWith(elements, properties);
    }
    return statement.copyWith(elements, properties.withSchemaId(schema.id));
  }

  private static TableElements buildElements(
      final Schema schema,
      final ConfiguredStatement<CreateSource> preparedStatement
  ) {
    try {
      throwOnInvalidSchema(schema);
      // custom types cannot be injected, so we can pass in an EMPTY type registry
      return SchemaParser.parse(FORMATTER.format(schema), TypeRegistry.EMPTY);
    } catch (final Exception e) {
      throw new KaypherStatementException(
          "Failed to convert schema to KAYPHER model: " + e.getMessage(),
          preparedStatement.getStatementText(),
          e);
    }
  }

  private static void throwOnInvalidSchema(final Schema schema) {
    SchemaConverters.connectToSqlConverter().toSqlType(schema);
  }

  private static PreparedStatement<CreateSource> buildPreparedStatement(
      final CreateSource stmt
  ) {
    return PreparedStatement.of(SqlFormatter.formatSql(stmt), stmt);
  }
}
