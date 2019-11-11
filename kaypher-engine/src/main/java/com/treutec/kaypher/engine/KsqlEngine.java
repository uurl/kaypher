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

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.KaypherExecutionContext;
import com.treutec.kaypher.ServiceInfo;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.internal.KaypherEngineMetrics;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.metastore.MetaStoreImpl;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.metrics.StreamsErrorCollector;
import com.treutec.kaypher.parser.KaypherParser.ParsedStatement;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.tree.ExecutableDdlStatement;
import com.treutec.kaypher.parser.tree.Query;
import com.treutec.kaypher.parser.tree.QueryContainer;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.query.id.QueryIdGenerator;
import com.treutec.kaypher.schema.registry.SchemaRegistryUtil;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.util.PersistentQueryMetadata;
import com.treutec.kaypher.util.QueryMetadata;
import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaypherEngine implements KaypherExecutionContext, Closeable {

  private static final Logger log = LoggerFactory.getLogger(KaypherEngine.class);

  private final Set<QueryMetadata> allLiveQueries = ConcurrentHashMap.newKeySet();
  private final KaypherEngineMetrics engineMetrics;
  private final ScheduledExecutorService aggregateMetricsCollector;
  private final String serviceId;
  private final EngineContext primaryContext;

  public KaypherEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final ServiceInfo serviceInfo,
      final QueryIdGenerator queryIdGenerator
  ) {
    this(
        serviceContext,
        processingLogContext,
        serviceInfo.serviceId(),
        new MetaStoreImpl(functionRegistry),
        (engine) -> new KaypherEngineMetrics(
            serviceInfo.metricsPrefix(),
            engine,
            serviceInfo.customMetricsTags(),
            serviceInfo.metricsExtension()
        ),
        queryIdGenerator);
  }

  public KaypherEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final String serviceId,
      final MutableMetaStore metaStore,
      final Function<KaypherEngine, KaypherEngineMetrics> engineMetricsFactory,
      final QueryIdGenerator queryIdGenerator
  ) {
    this.primaryContext = EngineContext.create(
        serviceContext,
        processingLogContext,
        metaStore,
        queryIdGenerator,
        this::unregisterQuery);
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
    this.engineMetrics = engineMetricsFactory.apply(this);
    this.aggregateMetricsCollector = Executors.newSingleThreadScheduledExecutor();
    this.aggregateMetricsCollector.scheduleAtFixedRate(
        () -> {
          try {
            this.engineMetrics.updateMetrics();
          } catch (final Exception e) {
            log.info("Error updating engine metrics", e);
          }
        },
        1000,
        1000,
        TimeUnit.MILLISECONDS
    );
  }

  public int numberOfLiveQueries() {
    return allLiveQueries.size();
  }

  @Override
  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return primaryContext.getPersistentQuery(queryId);
  }

  public List<PersistentQueryMetadata> getPersistentQueries() {
    return ImmutableList.copyOf(primaryContext.getPersistentQueries().values());
  }

  public boolean hasActiveQueries() {
    return !primaryContext.getPersistentQueries().isEmpty();
  }

  @Override
  public MetaStore getMetaStore() {
    return primaryContext.getMetaStore();
  }

  @Override
  public ServiceContext getServiceContext() {
    return primaryContext.getServiceContext();
  }

  public String getServiceId() {
    return serviceId;
  }

  @Override
  public KaypherExecutionContext createSandbox(final ServiceContext serviceContext) {
    return new SandboxedExecutionContext(primaryContext, serviceContext);
  }

  @Override
  public List<ParsedStatement> parse(final String sql) {
    return primaryContext.parse(sql);
  }

  @Override
  public PreparedStatement<?> prepare(final ParsedStatement stmt) {
    return primaryContext.prepare(stmt);
  }

  @Override
  public ExecuteResult execute(
      final ServiceContext serviceContext,
      final ConfiguredStatement<?> statement
  ) {
    final ExecuteResult result = EngineExecutor
        .create(primaryContext, serviceContext, statement.getConfig(), statement.getOverrides())
        .execute(statement);

    result.getQuery().ifPresent(this::registerQuery);

    return result;
  }

  @Override
  public void close() {
    allLiveQueries.forEach(QueryMetadata::close);
    engineMetrics.close();
    aggregateMetricsCollector.shutdown();
  }

  /**
   * Determines if a statement is executable by the engine.
   *
   * @param statement the statement to test.
   * @return {@code true} if the engine can execute the statement, {@code false} otherwise
   */
  public static boolean isExecutableStatement(final Statement statement) {
    return statement instanceof ExecutableDdlStatement
        || statement instanceof QueryContainer
        || statement instanceof Query;
  }

  private void registerQuery(final QueryMetadata query) {
    allLiveQueries.add(query);
    engineMetrics.registerQuery(query);
  }

  private void unregisterQuery(final ServiceContext serviceContext, final QueryMetadata query) {
    final String applicationId = query.getQueryApplicationId();

    if (!query.getState().equalsIgnoreCase("NOT_RUNNING")) {
      throw new IllegalStateException("query not stopped."
          + " id " + applicationId + ", state: " + query.getState());
    }

    if (!allLiveQueries.remove(query)) {
      return;
    }

    if (query.hasEverBeenStarted()) {
      SchemaRegistryUtil
          .cleanUpInternalTopicAvroSchemas(applicationId, serviceContext.getSchemaRegistryClient());
      serviceContext.getTopicClient().deleteInternalTopics(applicationId);
    }

    StreamsErrorCollector.notifyApplicationClose(applicationId);
  }
}
