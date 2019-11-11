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
package com.treutec.kaypher.ddl.commands;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.treutec.kaypher.execution.ddl.commands.CreateStreamCommand;
import com.treutec.kaypher.execution.ddl.commands.CreateTableCommand;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.logging.processing.NoopProcessingLogContext;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.properties.with.CreateSourceProperties;
import com.treutec.kaypher.parser.tree.CreateSource;
import com.treutec.kaypher.parser.tree.CreateStream;
import com.treutec.kaypher.parser.tree.CreateTable;
import com.treutec.kaypher.parser.tree.TableElement.Namespace;
import com.treutec.kaypher.parser.tree.TableElements;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.PhysicalSchema;
import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.GenericRowSerDe;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.SerdeOptions;
import com.treutec.kaypher.serde.ValueSerdeFactory;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.topic.TopicFactory;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.SchemaUtil;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicy;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicyFactory;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class CreateSourceFactory {
  private final ServiceContext serviceContext;
  private final SerdeOptionsSupplier serdeOptionsSupplier;
  private final ValueSerdeFactory serdeFactory;

  public CreateSourceFactory(final ServiceContext serviceContext) {
    this(serviceContext, SerdeOptions::buildForCreateStatement, new GenericRowSerDe());
  }

  @VisibleForTesting
  CreateSourceFactory(
      final ServiceContext serviceContext,
      final SerdeOptionsSupplier serdeOptionsSupplier,
      final ValueSerdeFactory serdeFactory
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.serdeOptionsSupplier =
        Objects.requireNonNull(serdeOptionsSupplier, "serdeOptionsSupplier");
    this.serdeFactory = Objects.requireNonNull(serdeFactory, "serdeFactory");
  }

  public CreateStreamCommand createStreamCommand(
      final String statementText,
      final CreateStream statement,
      final KaypherConfig kaypherConfig
  ) {
    final SourceName sourceName = statement.getName();
    final KaypherTopic topic = buildTopic(statement.getProperties(), serviceContext);
    final LogicalSchema schema = buildSchema(statement.getElements());
    final Optional<ColumnName> keyFieldName = buildKeyFieldName(statement, schema);
    final TimestampExtractionPolicy timestampExtractionPolicy = buildTimestampExtractor(
        kaypherConfig,
        statement.getProperties(),
        schema
    );
    final Set<SerdeOption> serdeOptions = serdeOptionsSupplier.build(
        schema,
        topic.getValueFormat().getFormat(),
        statement.getProperties().getWrapSingleValues(),
        kaypherConfig
    );
    validateSerdeCanHandleSchemas(
        kaypherConfig,
        serviceContext,
        serdeFactory,
        PhysicalSchema.from(schema, serdeOptions),
        topic
    );
    return new CreateStreamCommand(
        statementText,
        sourceName,
        schema,
        keyFieldName,
        timestampExtractionPolicy,
        serdeOptions,
        topic
    );
  }

  public CreateTableCommand createTableCommand(
      final String statementText,
      final CreateTable statement,
      final KaypherConfig kaypherConfig
  ) {
    final SourceName sourceName = statement.getName();
    final KaypherTopic topic = buildTopic(statement.getProperties(), serviceContext);
    final LogicalSchema schema = buildSchema(statement.getElements());
    final Optional<ColumnName> keyFieldName = buildKeyFieldName(statement, schema);
    final TimestampExtractionPolicy timestampExtractionPolicy = buildTimestampExtractor(
        kaypherConfig,
        statement.getProperties(),
        schema
    );
    final Set<SerdeOption> serdeOptions = serdeOptionsSupplier.build(
        schema,
        topic.getValueFormat().getFormat(),
        statement.getProperties().getWrapSingleValues(),
        kaypherConfig
    );
    validateSerdeCanHandleSchemas(
        kaypherConfig,
        serviceContext,
        serdeFactory,
        PhysicalSchema.from(schema, serdeOptions),
        topic
    );
    return new CreateTableCommand(
        statementText,
        sourceName,
        schema,
        keyFieldName,
        timestampExtractionPolicy,
        serdeOptions,
        topic
    );
  }

  private static Optional<ColumnName> buildKeyFieldName(
      final CreateSource statement,
      final LogicalSchema schema) {
    if (statement.getProperties().getKeyField().isPresent()) {
      final ColumnRef column = statement.getProperties().getKeyField().get();
      schema.findValueColumn(column)
          .orElseThrow(() -> new KaypherException(
              "The KEY column set in the WITH clause does not exist in the schema: '"
                  + column.toString(FormatOptions.noEscape()) + "'"
          ));
      return Optional.of(column.name());
    } else {
      return Optional.empty();
    }
  }

  private static LogicalSchema buildSchema(final TableElements tableElements) {
    if (Iterables.isEmpty(tableElements)) {
      throw new KaypherException("The statement does not define any columns.");
    }

    tableElements.forEach(e -> {
      if (e.getName().equals(SchemaUtil.ROWTIME_NAME)) {
        throw new KaypherException("'" + e.getName().name() + "' is a reserved column name.");
      }

      final boolean isRowKey = e.getName().equals(SchemaUtil.ROWKEY_NAME);

      if (e.getNamespace() == Namespace.KEY) {
        if (!isRowKey) {
          throw new KaypherException("'" + e.getName().name() + "' is an invalid KEY column name. "
              + "KAYPHER currently only supports KEY columns named ROWKEY.");
        }

        if (e.getType().getSqlType().baseType() != SqlBaseType.STRING) {
          throw new KaypherException("'" + e.getName().name()
              + "' is a KEY column with an unsupported type. "
              + "KAYPHER currently only supports KEY columns of type " + SqlBaseType.STRING + ".");
        }
      } else if (isRowKey) {
        throw new KaypherException("'" + e.getName().name() + "' is a reserved column name. "
            + "It can only be used for KEY columns.");
      }
    });

    return tableElements.toLogicalSchema(true);
  }

  private static KaypherTopic buildTopic(
      final CreateSourceProperties properties,
      final ServiceContext serviceContext
  ) {
    final String kafkaTopicName = properties.getKafkaTopic();
    if (!serviceContext.getTopicClient().isTopicExists(kafkaTopicName)) {
      throw new KaypherException("Kafka topic does not exist: " + kafkaTopicName);
    }

    return TopicFactory.create(properties);
  }

  private static TimestampExtractionPolicy buildTimestampExtractor(
      final KaypherConfig kaypherConfig,
      final CreateSourceProperties properties,
      final LogicalSchema schema
  ) {
    final Optional<ColumnRef> timestampName = properties.getTimestampColumnName();
    final Optional<String> timestampFormat = properties.getTimestampFormat();
    return TimestampExtractionPolicyFactory
        .create(kaypherConfig, schema, timestampName, timestampFormat);
  }

  private static void validateSerdeCanHandleSchemas(
      final KaypherConfig kaypherConfig,
      final ServiceContext serviceContext,
      final ValueSerdeFactory valueSerdeFactory,
      final PhysicalSchema physicalSchema,
      final KaypherTopic topic
  ) {
    valueSerdeFactory.create(
        topic.getValueFormat().getFormatInfo(),
        physicalSchema.valueSchema(),
        kaypherConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    ).close();
  }

  @FunctionalInterface
  interface SerdeOptionsSupplier {

    Set<SerdeOption> build(
        LogicalSchema schema,
        Format valueFormat,
        Optional<Boolean> wrapSingleValues,
        KaypherConfig kaypherConfig
    );
  }
}
