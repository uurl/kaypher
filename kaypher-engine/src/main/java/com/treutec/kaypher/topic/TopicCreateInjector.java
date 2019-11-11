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
import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.KaypherExecutionContext;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.parser.SqlFormatter;
import com.treutec.kaypher.parser.properties.with.CreateSourceAsProperties;
import com.treutec.kaypher.parser.properties.with.CreateSourceProperties;
import com.treutec.kaypher.parser.tree.CreateAsSelect;
import com.treutec.kaypher.parser.tree.CreateSource;
import com.treutec.kaypher.parser.tree.CreateTable;
import com.treutec.kaypher.parser.tree.CreateTableAsSelect;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.properties.with.CommonCreateConfigs;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.statement.Injector;
import com.treutec.kaypher.topic.TopicProperties.Builder;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.config.TopicConfig;

/**
 * An injector which injects the topic name, number of partitions and number of
 * replicas into the topic properties of the supplied {@code statement}.
 *
 * <p>If a statement that is not {@code CreateAsSelect} or {@code CreateSource }
 * is passed in, this results in a no-op that returns the incoming statement.</p>
 *
 * @see TopicProperties.Builder
 */
public class TopicCreateInjector implements Injector {

  private final KafkaTopicClient topicClient;
  private final MetaStore metaStore;

  public TopicCreateInjector(
      final KaypherExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    this(serviceContext.getTopicClient(), executionContext.getMetaStore());
  }

  TopicCreateInjector(
      final KafkaTopicClient topicClient,
      final MetaStore metaStore) {
    this.topicClient = Objects.requireNonNull(topicClient, "topicClient");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement
  ) {
    return inject(statement, new TopicProperties.Builder());
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement,
      final TopicProperties.Builder topicPropertiesBuilder
  ) {
    if (statement.getStatement() instanceof CreateAsSelect) {
      return (ConfiguredStatement<T>) injectForCreateAsSelect(
          (ConfiguredStatement<? extends CreateAsSelect>) statement,
          topicPropertiesBuilder);
    }

    if (statement.getStatement() instanceof CreateSource) {
      return (ConfiguredStatement<T>) injectForCreateSource(
          (ConfiguredStatement<? extends CreateSource>) statement,
          topicPropertiesBuilder);
    }

    return statement;
  }

  private ConfiguredStatement<? extends CreateSource> injectForCreateSource(
      final ConfiguredStatement<? extends CreateSource> statement,
      final TopicProperties.Builder topicPropertiesBuilder
  ) {
    final CreateSource createSource = statement.getStatement();
    final CreateSourceProperties properties = createSource.getProperties();

    final String topicName = properties.getKafkaTopic();

    if (topicClient.isTopicExists(topicName)) {
      topicPropertiesBuilder.withSource(() -> topicClient.describeTopic(topicName));
    } else if (!properties.getPartitions().isPresent()) {
      final CreateSource example = createSource.copyWith(
          createSource.getElements(),
          properties.withPartitionsAndReplicas(2, (short) 1));
      throw new KaypherException(
          "Topic '" + topicName + "' does not exist. If you want to create a new topic for the "
              + "stream/table please re-run the statement providing the required '"
              + CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS + "' configuration in the WITH "
              + "clause (and optionally '" + CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS + "'). "
              + "For example: " + SqlFormatter.formatSql(example));
    }

    topicPropertiesBuilder
        .withName(topicName)
        .withWithClause(
            Optional.of(properties.getKafkaTopic()),
            properties.getPartitions(),
            properties.getReplicas());

    createTopic(topicPropertiesBuilder, statement, createSource instanceof CreateTable);

    return statement;
  }

  @SuppressWarnings("unchecked")
  private <T extends CreateAsSelect> ConfiguredStatement<?> injectForCreateAsSelect(
      final ConfiguredStatement<T> statement,
      final TopicProperties.Builder topicPropertiesBuilder
  ) {
    final String prefix =
        statement.getOverrides().getOrDefault(
            KaypherConfig.KAYPHER_OUTPUT_TOPIC_NAME_PREFIX_CONFIG,
            statement.getConfig().getString(KaypherConfig.KAYPHER_OUTPUT_TOPIC_NAME_PREFIX_CONFIG))
            .toString();

    final T createAsSelect = statement.getStatement();
    final CreateSourceAsProperties properties = createAsSelect.getProperties();

    final SourceTopicsExtractor extractor = new SourceTopicsExtractor(metaStore);
    extractor.process(statement.getStatement().getQuery(), null);
    final String sourceTopicName = extractor.getPrimaryKafkaTopicName();

    topicPropertiesBuilder
        .withName(prefix + createAsSelect.getName().name())
        .withSource(() -> topicClient.describeTopic(sourceTopicName))
        .withWithClause(
            properties.getKafkaTopic(),
            properties.getPartitions(),
            properties.getReplicas());

    final boolean shouldCompactTopic = createAsSelect instanceof CreateTableAsSelect
        && !createAsSelect.getQuery().getWindow().isPresent();

    final TopicProperties info = createTopic(topicPropertiesBuilder, statement, shouldCompactTopic);

    final T withTopic = (T) createAsSelect.copyWith(properties.withTopic(
        info.getTopicName(),
        info.getPartitions(),
        info.getReplicas()
    ));

    final String withTopicText = SqlFormatter.formatSql(withTopic) + ";";

    return statement.withStatement(withTopicText, withTopic);
  }

  private TopicProperties createTopic(
      final Builder topicPropertiesBuilder,
      final ConfiguredStatement<?> statement,
      final boolean shouldCompactTopic
  ) {
    final TopicProperties info = topicPropertiesBuilder
        .withOverrides(statement.getOverrides())
        .withKaypherConfig(statement.getConfig())
        .build();

    final Map<String, ?> config = shouldCompactTopic
        ? ImmutableMap.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        : Collections.emptyMap();

    topicClient.createTopic(info.getTopicName(), info.getPartitions(), info.getReplicas(), config);

    return info;
  }
}
