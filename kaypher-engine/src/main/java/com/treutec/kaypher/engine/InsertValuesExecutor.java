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
package com.treutec.kaypher.engine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.KaypherExecutionContext;
import com.treutec.kaypher.exception.KaypherTopicAuthorizationException;
import com.treutec.kaypher.execution.codegen.CodeGenRunner;
import com.treutec.kaypher.execution.codegen.ExpressionMetadata;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.VisitParentExpressionVisitor;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.logging.processing.NoopProcessingLogContext;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.parser.tree.InsertValues;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.DefaultSqlValueCoercer;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.PhysicalSchema;
import com.treutec.kaypher.schema.kaypher.SchemaConverters;
import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import com.treutec.kaypher.schema.kaypher.SqlValueCoercer;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.GenericKeySerDe;
import com.treutec.kaypher.serde.GenericRowSerDe;
import com.treutec.kaypher.serde.KeySerdeFactory;
import com.treutec.kaypher.serde.ValueSerdeFactory;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherConstants;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.KaypherStatementException;
import com.treutec.kaypher.util.SchemaUtil;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class InsertValuesExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Duration MAX_SEND_TIMEOUT = Duration.ofSeconds(5);

  private final LongSupplier clock;
  private final boolean canBeDisabledByConfig;
  private final RecordProducer producer;
  private final ValueSerdeFactory valueSerdeFactory;
  private final KeySerdeFactory keySerdeFactory;

  public InsertValuesExecutor() {
    this(true, InsertValuesExecutor::sendRecord);
  }

  public interface RecordProducer {

    void sendRecord(
        ProducerRecord<byte[], byte[]> record,
        ServiceContext serviceContext,
        Map<String, Object> producerProps
    );
  }

  @VisibleForTesting
  InsertValuesExecutor(
      final boolean canBeDisabledByConfig,
      final RecordProducer producer
  ) {
    this(
        producer,
        canBeDisabledByConfig,
        System::currentTimeMillis,
        new GenericKeySerDe(),
        new GenericRowSerDe()
    );
  }

  @VisibleForTesting
  InsertValuesExecutor(
      final LongSupplier clock,
      final KeySerdeFactory keySerdeFactory,
      final ValueSerdeFactory valueSerdeFactory
  ) {
    this(InsertValuesExecutor::sendRecord, true, clock, keySerdeFactory, valueSerdeFactory);
  }

  private InsertValuesExecutor(
      final RecordProducer producer,
      final boolean canBeDisabledByConfig,
      final LongSupplier clock,
      final KeySerdeFactory keySerdeFactory,
      final ValueSerdeFactory valueSerdeFactory
  ) {
    this.canBeDisabledByConfig = canBeDisabledByConfig;
    this.producer = Objects.requireNonNull(producer, "producer");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.keySerdeFactory = Objects.requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.valueSerdeFactory = Objects.requireNonNull(valueSerdeFactory, "valueSerdeFactory");
  }

  public void execute(
      final ConfiguredStatement<InsertValues> statement,
      final Map<String, ?> sessionProperties,
      final KaypherExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final InsertValues insertValues = statement.getStatement();
    final KaypherConfig config = statement.getConfig()
        .cloneWithPropertyOverwrite(statement.getOverrides());

    final ProducerRecord<byte[], byte[]> record =
        buildRecord(statement, executionContext, serviceContext);

    try {
      producer.sendRecord(record, serviceContext, config.getProducerClientConfigProps());
    } catch (final TopicAuthorizationException e) {
      // TopicAuthorizationException does not give much detailed information about why it failed,
      // except which topics are denied. Here we just add the ACL to make the error message
      // consistent with other authorization error messages.
      final Exception rootCause = new KaypherTopicAuthorizationException(
          AclOperation.WRITE,
          e.unauthorizedTopics()
      );

      throw new KaypherException(createInsertFailedExceptionMessage(insertValues), rootCause);
    } catch (final Exception e) {
      throw new KaypherException(createInsertFailedExceptionMessage(insertValues), e);
    }
  }

  private ProducerRecord<byte[], byte[]> buildRecord(
      final ConfiguredStatement<InsertValues> statement,
      final KaypherExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    throwIfDisabled(statement.getConfig());

    final InsertValues insertValues = statement.getStatement();
    final KaypherConfig config = statement.getConfig()
        .cloneWithPropertyOverwrite(statement.getOverrides());

    final DataSource<?> dataSource = executionContext
        .getMetaStore()
        .getSource(insertValues.getTarget());

    if (dataSource == null) {
      throw new KaypherException("Cannot insert values into an unknown stream/table: "
          + insertValues.getTarget());
    }

    if (dataSource.getKaypherTopic().getKeyFormat().isWindowed()) {
      throw new KaypherException("Cannot insert values into windowed stream/table!");
    }

    try {
      final RowData row = extractRow(
          insertValues,
          dataSource,
          executionContext.getMetaStore(),
          config);

      final byte[] key = serializeKey(row.key, dataSource, config, serviceContext);
      final byte[] value = serializeValue(row.value, dataSource, config, serviceContext);

      final String topicName = dataSource.getKafkaTopicName();

      return new ProducerRecord<>(
          topicName,
          null,
          row.ts,
          key,
          value
      );
    } catch (Exception e) {
      throw new KaypherStatementException(
          createInsertFailedExceptionMessage(insertValues) + " " + e.getMessage(),
          statement.getStatementText(),
          e);
    }
  }

  private static String createInsertFailedExceptionMessage(final InsertValues insertValues) {
    return "Failed to insert values into '" + insertValues.getTarget().name() + "'.";
  }

  private void throwIfDisabled(final KaypherConfig config) {
    final boolean isEnabled = config.getBoolean(KaypherConfig.KAYPHER_INSERT_INTO_VALUES_ENABLED);

    if (canBeDisabledByConfig && !isEnabled) {
      throw new KaypherException("The server has disabled INSERT INTO ... VALUES functionality. "
          + "To enable it, restart your KAYPHER-server with 'kaypher.insert.into.values.enabled'=true");
    }
  }

  private RowData extractRow(
      final InsertValues insertValues,
      final DataSource<?> dataSource,
      final FunctionRegistry functionRegistry,
      final KaypherConfig config
  ) {
    final List<ColumnName> columns = insertValues.getColumns().isEmpty()
        ? implicitColumns(dataSource, insertValues.getValues())
        : insertValues.getColumns();

    final LogicalSchema schema = dataSource.getSchema();

    final Map<ColumnName, Object> values = resolveValues(
        insertValues, columns, schema, functionRegistry, config);

    handleExplicitKeyField(values, dataSource.getKeyField());

    if (dataSource.getDataSourceType() == DataSourceType.KTABLE
        && values.get(SchemaUtil.ROWKEY_NAME) == null) {
      throw new KaypherException("Value for ROWKEY is required for tables");
    }

    final long ts = (long) values.getOrDefault(SchemaUtil.ROWTIME_NAME, clock.getAsLong());

    final Struct key = buildKey(schema, values);
    final GenericRow value = buildValue(schema, values);

    return RowData.of(ts, key, value);
  }

  private static Struct buildKey(
      final LogicalSchema schema,
      final Map<ColumnName, Object> values
  ) {

    final Struct key = new Struct(schema.keyConnectSchema());

    for (final org.apache.kafka.connect.data.Field field : key.schema().fields()) {
      final Object value = values.get(ColumnName.of(field.name()));
      key.put(field, value);
    }

    return key;
  }

  private static GenericRow buildValue(
      final LogicalSchema schema,
      final Map<ColumnName, Object> values
  ) {
    return new GenericRow(
        schema
            .value()
            .stream()
            .map(Column::name)
            .map(values::get)
            .collect(Collectors.toList())
    );
  }

  @SuppressWarnings("UnstableApiUsage")
  private static List<ColumnName> implicitColumns(
      final DataSource<?> dataSource,
      final List<Expression> values
  ) {
    final LogicalSchema schema = dataSource.getSchema();

    final List<ColumnName> fieldNames = Streams.concat(
        schema.key().stream(),
        schema.value().stream())
        .map(Column::name)
        .collect(Collectors.toList());

    if (fieldNames.size() != values.size()) {
      throw new KaypherException(
          "Expected a value for each column."
              + " Expected Columns: " + fieldNames
              + ". Got " + values);
    }

    return fieldNames;
  }

  private static Map<ColumnName, Object> resolveValues(
      final InsertValues insertValues,
      final List<ColumnName> columns,
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry,
      final KaypherConfig config
  ) {
    final Map<ColumnName, Object> values = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      final ColumnName column = columns.get(i);
      final SqlType columnType = columnType(column, schema);
      final Expression valueExp = insertValues.getValues().get(i);

      final Object value =
          new ExpressionResolver(columnType, column, schema, functionRegistry, config)
          .process(valueExp, null);

      values.put(column, value);
    }
    return values;
  }

  private static void handleExplicitKeyField(
      final Map<ColumnName, Object> values,
      final KeyField keyField
  ) {
    final Optional<ColumnRef> keyFieldName = keyField.ref();
    if (keyFieldName.isPresent()) {
      final ColumnRef key = keyFieldName.get();
      final Object keyValue = values.get(key.name());
      final Object rowKeyValue = values.get(SchemaUtil.ROWKEY_NAME);

      if (keyValue != null ^ rowKeyValue != null) {
        if (keyValue == null) {
          values.put(key.name(), rowKeyValue);
        } else {
          values.put(SchemaUtil.ROWKEY_NAME, keyValue.toString());
        }
      } else if (keyValue != null && !Objects.equals(keyValue.toString(), rowKeyValue)) {
        throw new KaypherException(String.format(
            "Expected ROWKEY and %s to match but got %s and %s respectively.",
            key.toString(FormatOptions.noEscape()), rowKeyValue, keyValue));
      }
    }
  }

  private static SqlType columnType(final ColumnName column, final LogicalSchema schema) {
    return schema
        .findColumn(ColumnRef.withoutSource(column))
        .map(Column::type)
        .orElseThrow(IllegalStateException::new);
  }

  private byte[] serializeKey(
      final Struct keyValue,
      final DataSource<?> dataSource,
      final KaypherConfig config,
      final ServiceContext serviceContext
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        dataSource.getSchema(),
        dataSource.getSerdeOptions()
    );

    final Serde<Struct> keySerde = keySerdeFactory.create(
        dataSource.getKaypherTopic().getKeyFormat().getFormatInfo(),
        physicalSchema.keySchema(),
        config,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    );

    try {
      return keySerde
          .serializer()
          .serialize(dataSource.getKafkaTopicName(), keyValue);
    } catch (final Exception e) {
      throw new KaypherException("Could not serialize key: " + keyValue, e);
    }
  }

  private byte[] serializeValue(
      final GenericRow row,
      final DataSource<?> dataSource,
      final KaypherConfig config,
      final ServiceContext serviceContext
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        dataSource.getSchema(),
        dataSource.getSerdeOptions()
    );

    final Serde<GenericRow> valueSerde = valueSerdeFactory.create(
        dataSource.getKaypherTopic().getValueFormat().getFormatInfo(),
        physicalSchema.valueSchema(),
        config,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    );

    final String topicName = dataSource.getKafkaTopicName();

    try {
      return valueSerde.serializer().serialize(topicName, row);
    } catch (final Exception e) {
      if (dataSource.getKaypherTopic().getValueFormat().getFormat() == Format.AVRO) {
        final Throwable rootCause = ExceptionUtils.getRootCause(e);
        if (rootCause instanceof RestClientException) {
          switch (((RestClientException) rootCause).getStatus()) {
            case HttpStatus.SC_UNAUTHORIZED:
            case HttpStatus.SC_FORBIDDEN:
              throw new KaypherException(String.format(
                  "Not authorized to write Schema Registry subject: [%s]",
                  topicName + KaypherConstants.SCHEMA_REGISTRY_VALUE_SUFFIX
              ));
            default:
              break;
          }
        }
      }

      throw new KaypherException("Could not serialize row: " + row, e);
    }
  }

  @SuppressWarnings("TryFinallyCanBeTryWithResources")
  private static void sendRecord(
      final ProducerRecord<byte[], byte[]> record,
      final ServiceContext serviceContext,
      final Map<String, Object> producerProps
  ) {
    // for now, just create a new producer each time
    final Producer<byte[], byte[]> producer = serviceContext
        .getKafkaClientSupplier()
        .getProducer(producerProps);

    final Future<RecordMetadata> producerCallResult;

    try {
      producerCallResult = producer.send(record);
    } finally {
      producer.close(MAX_SEND_TIMEOUT);
    }

    try {
      // Check if the producer failed to write to the topic. This can happen if the
      // ServiceContext does not have write permissions.
      producerCallResult.get();
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static final class RowData {

    final long ts;
    final Struct key;
    final GenericRow value;

    private static RowData of(final long ts, final Struct key, final GenericRow value) {
      return new RowData(ts, key, value);
    }

    private RowData(final long ts, final Struct key, final GenericRow value) {
      this.ts = ts;
      this.key = key;
      this.value = value;
    }
  }

  private static class ExpressionResolver extends VisitParentExpressionVisitor<Object, Void> {

    private final SqlType fieldType;
    private final ColumnName fieldName;
    private final LogicalSchema schema;
    private final SqlValueCoercer defaultSqlValueCoercer = new DefaultSqlValueCoercer();
    private final FunctionRegistry functionRegistry;
    private final KaypherConfig config;

    ExpressionResolver(
        final SqlType fieldType,
        final ColumnName fieldName,
        final LogicalSchema schema,
        final FunctionRegistry functionRegistry,
        final KaypherConfig config
    ) {
      this.fieldType = Objects.requireNonNull(fieldType, "fieldType");
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
      this.schema = Objects.requireNonNull(schema, "schema");
      this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
      this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    protected Object visitExpression(final Expression expression, final Void context) {
      final ExpressionMetadata metadata =
          Iterables.getOnlyElement(
              CodeGenRunner.compileExpressions(
                  Stream.of(expression),
                  "insert value",
                  schema,
                  config,
                  functionRegistry)
          );

      // we expect no column references, so we can pass in an empty generic row
      final Object value = metadata.evaluate(new GenericRow());

      return defaultSqlValueCoercer.coerce(value, fieldType)
          .orElseThrow(() -> {
            final SqlBaseType valueSqlType = SchemaConverters.javaToSqlConverter()
                .toSqlType(value.getClass());

            return new KaypherException(
                String.format("Expected type %s for field %s but got %s(%s)",
                    fieldType,
                    fieldName,
                    valueSqlType,
                    value));

          });
    }
  }
}