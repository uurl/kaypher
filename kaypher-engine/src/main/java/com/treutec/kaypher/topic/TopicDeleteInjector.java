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

package com.treutec.kaypher.topic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import com.treutec.kaypher.KaypherExecutionContext;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.SqlFormatter;
import com.treutec.kaypher.parser.tree.DropStatement;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.schema.registry.SchemaRegistryUtil;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.statement.Injector;
import com.treutec.kaypher.util.ExecutorUtil;
import com.treutec.kaypher.util.KaypherConstants;
import com.treutec.kaypher.util.KaypherException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This {@code Injector} will delete the topic associated with a
 * {@code DROP [STREAM/TABLE] X DELETE TOPIC}. Any other statements are just
 * passed through. Furthermore, it will remove the DELETE TOPIC clause from
 * the statement, indicating that the operation has already been done.
 *
 * <p>If the topic being deleted is {@link Format#AVRO},
 * this injector will also clean up the corresponding schema in the schema
 * registry.
 */
public class TopicDeleteInjector implements Injector {

  private final MetaStore metastore;
  private final KafkaTopicClient topicClient;
  private final SchemaRegistryClient schemaRegistryClient;

  private static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;

  public TopicDeleteInjector(
      final KaypherExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    this(
        Objects.requireNonNull(executionContext, "executionContext").getMetaStore(),
        serviceContext.getTopicClient(),
        serviceContext.getSchemaRegistryClient()
    );
  }

  @VisibleForTesting
  TopicDeleteInjector(
      final MetaStore metastore,
      final KafkaTopicClient serviceContext,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    this.metastore = Objects.requireNonNull(metastore, "metastore");
    this.topicClient = Objects.requireNonNull(serviceContext, "topicClient");
    this.schemaRegistryClient =
        Objects.requireNonNull(schemaRegistryClient, "schemaRegistryClient");
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement) {
    if (!(statement.getStatement() instanceof DropStatement)) {
      return statement;
    }

    final DropStatement dropStatement = (DropStatement) statement.getStatement();
    if (!dropStatement.isDeleteTopic()) {
      return statement;
    }

    final SourceName sourceName = dropStatement.getName();
    final DataSource<?> source = metastore.getSource(sourceName);

    if (source != null) {
      checkTopicRefs(source);
      try {
        ExecutorUtil.executeWithRetries(
            () -> topicClient.deleteTopics(ImmutableList.of(source.getKafkaTopicName())),
            ExecutorUtil.RetryBehaviour.ALWAYS);
      } catch (Exception e) {
        throw new RuntimeException("Could not delete the corresponding kafka topic: "
                + source.getKafkaTopicName(), e);
      }

      try {
        if (source.getKaypherTopic().getValueFormat().getFormat() == Format.AVRO) {
          SchemaRegistryUtil.deleteSubjectWithRetries(
                  schemaRegistryClient,
                  source.getKafkaTopicName() + KaypherConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
        }
      } catch (final Exception e) {
        checkSchemaError(e, source.getKafkaTopicName());
      }
    } else if (dropStatement.getIfExists()) {
      throw new KaypherException("Could not find source to delete topic for: " + statement);
    }

    final T withoutDelete = (T) dropStatement.withoutDeleteClause();
    final String withoutDeleteText = SqlFormatter.formatSql(withoutDelete) + ";";

    return statement.withStatement(withoutDeleteText, withoutDelete);
  }

  private void checkSchemaError(final Exception error, final String sourceName) {
    if (!(error instanceof RestClientException
            && ((RestClientException) error).getErrorCode() == SUBJECT_NOT_FOUND_ERROR_CODE)) {
      throw new RuntimeException("Could not clean up the schema registry for topic: "
              + sourceName, error);
    }
  }

  private void checkTopicRefs(final DataSource<?> source) {
    final String topicName = source.getKafkaTopicName();
    final SourceName sourceName = source.getName();
    final Map<SourceName, DataSource<?>> sources = metastore.getAllDataSources();
    final String using = sources.values().stream()
        .filter(s -> s.getKafkaTopicName().equals(topicName))
        .map(DataSource::getName)
        .filter(name -> !sourceName.equals(name))
        .map(SourceName::name)
        .sorted()
        .collect(Collectors.joining(", "));
    if (!using.isEmpty()) {
      throw new RuntimeException(
          String.format(
              "Refusing to delete topic. Found other data sources (%s) using topic %s",
              using,
              topicName
          )
      );
    }
  }
}
