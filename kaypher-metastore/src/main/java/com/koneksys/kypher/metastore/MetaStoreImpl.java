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

package com.koneksys.kaypher.metastore;

import com.koneksys.kaypher.function.AggregateFunctionFactory;
import com.koneksys.kaypher.function.FunctionRegistry;
import com.koneksys.kaypher.function.kaypherAggregateFunction;
import com.koneksys.kaypher.function.UdfFactory;
import com.koneksys.kaypher.metastore.model.kaypherTopic;
import com.koneksys.kaypher.metastore.model.StructuredDataSource;
import com.koneksys.kaypher.util.kaypherException;
import com.koneksys.kaypher.util.kaypherReferentialIntegrityException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.connect.data.Schema;

@ThreadSafe
public final class MetaStoreImpl implements MutableMetaStore {

  private final Map<String, kaypherTopic> topics = new ConcurrentHashMap<>();
  private final Map<String, SourceInfo> dataSources = new ConcurrentHashMap<>();
  private final Object referentialIntegrityLock = new Object();
  private final FunctionRegistry functionRegistry;

  public MetaStoreImpl(final FunctionRegistry functionRegistry) {
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  private MetaStoreImpl(
      final Map<String, kaypherTopic> topics,
      final Map<String, SourceInfo> dataSources,
      final FunctionRegistry functionRegistry
  ) {
    this.topics.putAll(topics);
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");

    dataSources.forEach((name, info) -> this.dataSources.put(name, info.copy()));
  }

  @Override
  public kaypherTopic getTopic(final String topicName) {
    return topics.get(topicName);
  }

  @Override
  public void putTopic(final kaypherTopic topic) {
    if (topics.putIfAbsent(topic.getName(), topic) != null) {
      throw new kaypherException(
          "Cannot add the new topic. Another topic with the same name already exists: "
          + topic.getName());
    }
  }

  @Override
  public StructuredDataSource<?> getSource(final String sourceName) {
    final SourceInfo source = dataSources.get(sourceName);
    if (source == null) {
      return null;
    }
    return source.source;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<StructuredDataSource<?>> getSourceForTopic(final String kaypherTopicName) {
    return (Optional)dataSources.values()
        .stream()
        .filter(p -> p.source.getkaypherTopicName() != null
            && p.source.getkaypherTopicName().equals(kaypherTopicName))
        .map(sourceInfo -> sourceInfo.source)
        .findFirst();
  }

  @Override
  public List<StructuredDataSource<?>> getSourcesForKafkaTopic(final String kafkaTopicName) {
    return dataSources.values()
        .stream()
        .filter(p -> p.source.getKafkaTopicName() != null
            && p.source.getKafkaTopicName().equals(kafkaTopicName))
        .map(sourceInfo -> sourceInfo.source)
        .collect(Collectors.toList());
  }

  @Override
  public void putSource(final StructuredDataSource<?> dataSource) {
    if (dataSources.putIfAbsent(dataSource.getName(), new SourceInfo(dataSource)) != null) {
      throw new kaypherException(
          "Cannot add the new data source. Another data source with the same name already exists: "
              + dataSource.toString());
    }
  }

  @Override
  public void deleteTopic(final String topicName) {
    if (topics.remove(topicName) == null) {
      throw new kaypherException(String.format("No topic with name %s was registered.", topicName));
    }
  }

  @Override
  public void deleteSource(final String sourceName) {
    synchronized (referentialIntegrityLock) {
      dataSources.compute(sourceName, (ignored, source) -> {
        if (source == null) {
          throw new kaypherException(String.format("No data source with name %s exists.", sourceName));
        }

        final String sourceForQueriesMessage = source.referentialIntegrity
            .getSourceForQueries()
            .stream()
            .collect(Collectors.joining(", "));

        final String sinkForQueriesMessage = source.referentialIntegrity
            .getSinkForQueries()
            .stream()
            .collect(Collectors.joining(", "));

        if (!sourceForQueriesMessage.isEmpty() || !sinkForQueriesMessage.isEmpty()) {
          throw new kaypherReferentialIntegrityException(
              String.format("Cannot drop %s.%n"
                      + "The following queries read from this source: [%s].%n"
                      + "The following queries write into this source: [%s].%n"
                      + "You need to terminate them before dropping %s.",
                  sourceName, sourceForQueriesMessage, sinkForQueriesMessage, sourceName));
        }

        return null;
      });
    }
  }

  @Override
  public Map<String, StructuredDataSource<?>> getAllStructuredDataSources() {
    return dataSources
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().source));
  }

  @Override
  public Map<String, kaypherTopic> getAllkaypherTopics() {
    return Collections.unmodifiableMap(topics);
  }

  @Override
  public void updateForPersistentQuery(
      final String queryId,
      final Set<String> sourceNames,
      final Set<String> sinkNames
  ) {
    synchronized (referentialIntegrityLock) {
      final String sourceAlreadyRegistered = streamSources(sourceNames)
          .filter(source -> source.referentialIntegrity.getSourceForQueries().contains(queryId))
          .map(source -> source.source.getName())
          .collect(Collectors.joining(","));

      final String sinkAlreadyRegistered = streamSources(sinkNames)
          .filter(source -> source.referentialIntegrity.getSinkForQueries().contains(queryId))
          .map(source -> source.source.getName())
          .collect(Collectors.joining(","));

      if (!sourceAlreadyRegistered.isEmpty() || !sinkAlreadyRegistered.isEmpty()) {
        throw new kaypherException("query already registered."
            + " queryId: " + queryId
            + ", registeredAgainstSource: " + sourceAlreadyRegistered
            + ", registeredAgainstSink: " + sinkAlreadyRegistered);
      }

      streamSources(sourceNames)
          .forEach(source -> source.referentialIntegrity.addSourceForQueries(queryId));
      streamSources(sinkNames)
          .forEach(source -> source.referentialIntegrity.addSinkForQueries(queryId));
    }
  }

  @Override
  public void removePersistentQuery(final String queryId) {
    synchronized (referentialIntegrityLock) {
      for (final SourceInfo sourceInfo : dataSources.values()) {
        sourceInfo.referentialIntegrity.removeQuery(queryId);
      }
    }
  }

  @Override
  public Set<String> getQueriesWithSource(final String sourceName) {
    final SourceInfo sourceInfo = dataSources.get(sourceName);
    if (sourceInfo == null) {
      return Collections.emptySet();
    }
    return sourceInfo.referentialIntegrity.getSourceForQueries();
  }

  @Override
  public Set<String> getQueriesWithSink(final String sourceName) {
    final SourceInfo sourceInfo = dataSources.get(sourceName);
    if (sourceInfo == null) {
      return Collections.emptySet();
    }
    return sourceInfo.referentialIntegrity.getSinkForQueries();
  }

  @Override
  public MutableMetaStore copy() {
    synchronized (referentialIntegrityLock) {
      return new MetaStoreImpl(topics, dataSources, functionRegistry);
    }
  }

  @Override
  public UdfFactory getUdfFactory(final String functionName) {
    return functionRegistry.getUdfFactory(functionName);
  }

  public boolean isAggregate(final String functionName) {
    return functionRegistry.isAggregate(functionName);
  }

  public kaypherAggregateFunction<?, ?> getAggregate(
      final String functionName,
      final Schema argumentType
  ) {
    return functionRegistry.getAggregate(functionName, argumentType);
  }

  @Override
  public List<UdfFactory> listFunctions() {
    return functionRegistry.listFunctions();
  }

  @Override
  public AggregateFunctionFactory getAggregateFactory(final String functionName) {
    return functionRegistry.getAggregateFactory(functionName);
  }

  @Override
  public List<AggregateFunctionFactory> listAggregateFunctions() {
    return functionRegistry.listAggregateFunctions();
  }

  private Stream<SourceInfo> streamSources(final Set<String> sourceNames) {
    return sourceNames.stream()
        .map(sourceName -> {
          final SourceInfo sourceInfo = dataSources.get(sourceName);
          if (sourceInfo == null) {
            throw new kaypherException("Unknown source: " + sourceName);
          }

          return sourceInfo;
        });
  }

  private static final class SourceInfo {

    private final StructuredDataSource<?> source;
    private final ReferentialIntegrityTableEntry referentialIntegrity;

    private SourceInfo(
        final StructuredDataSource<?> source
    ) {
      this.source = Objects.requireNonNull(source, "source");
      this.referentialIntegrity = new ReferentialIntegrityTableEntry();
    }

    private SourceInfo(
        final StructuredDataSource<?> source,
        final ReferentialIntegrityTableEntry referentialIntegrity
    ) {
      this.source = Objects.requireNonNull(source, "source");
      this.referentialIntegrity = referentialIntegrity.copy();
    }

    public SourceInfo copy() {
      return new SourceInfo(source, referentialIntegrity);
    }
  }
}
