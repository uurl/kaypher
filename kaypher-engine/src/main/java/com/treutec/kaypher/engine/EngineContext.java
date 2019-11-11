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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableSet;
import com.treutec.kaypher.ddl.commands.CommandFactories;
import com.treutec.kaypher.ddl.commands.DdlCommandExec;
import com.treutec.kaypher.engine.rewrite.AstSanitizer;
import com.treutec.kaypher.execution.ddl.commands.DdlCommand;
import com.treutec.kaypher.execution.ddl.commands.DdlCommandResult;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.parser.DefaultKaypherParser;
import com.treutec.kaypher.parser.KaypherParser;
import com.treutec.kaypher.parser.KaypherParser.ParsedStatement;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.tree.ExecutableDdlStatement;
import com.treutec.kaypher.query.QueryExecutor;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.query.id.QueryIdGenerator;
import com.treutec.kaypher.services.SandboxedServiceContext;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherStatementException;
import com.treutec.kaypher.util.PersistentQueryMetadata;
import com.treutec.kaypher.util.QueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Holds the mutable state and services of the engine.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
final class EngineContext {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final MutableMetaStore metaStore;
  private final ServiceContext serviceContext;
  private final CommandFactories ddlCommandFactory;
  private final DdlCommandExec ddlCommandExec;
  private final QueryIdGenerator queryIdGenerator;
  private final ProcessingLogContext processingLogContext;
  private final KaypherParser parser;
  private final BiConsumer<ServiceContext, QueryMetadata> outerOnQueryCloseCallback;
  private final Map<QueryId, PersistentQueryMetadata> persistentQueries;

  static EngineContext create(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MutableMetaStore metaStore,
      final QueryIdGenerator queryIdGenerator,
      final BiConsumer<ServiceContext, QueryMetadata> onQueryCloseCallback
  ) {
    return new EngineContext(
        serviceContext,
        processingLogContext,
        metaStore,
        queryIdGenerator,
        onQueryCloseCallback,
        new DefaultKaypherParser()
    );
  }

  private EngineContext(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final MutableMetaStore metaStore,
      final QueryIdGenerator queryIdGenerator,
      final BiConsumer<ServiceContext, QueryMetadata> onQueryCloseCallback,
      final KaypherParser parser
  ) {
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.metaStore = requireNonNull(metaStore, "metaStore");
    this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator");
    this.ddlCommandFactory = new CommandFactories(serviceContext, metaStore);
    this.outerOnQueryCloseCallback = requireNonNull(onQueryCloseCallback, "onQueryCloseCallback");
    this.ddlCommandExec = new DdlCommandExec(metaStore);
    this.persistentQueries = new ConcurrentHashMap<>();
    this.processingLogContext = requireNonNull(processingLogContext, "processingLogContext");
    this.parser = requireNonNull(parser, "parser");
  }

  EngineContext createSandbox(final ServiceContext serviceContext) {
    final EngineContext sandBox = EngineContext.create(
        SandboxedServiceContext.create(serviceContext),
        processingLogContext,
        metaStore.copy(),
        queryIdGenerator.createSandbox(),
        (sc, query) -> { /* No-op */ }
    );

    persistentQueries.forEach((queryId, query) ->
        sandBox.persistentQueries.put(
            query.getQueryId(),
            query.copyWith(sandBox::unregisterQuery)));

    return sandBox;
  }

  Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return Optional.ofNullable(persistentQueries.get(queryId));
  }

  Map<QueryId, PersistentQueryMetadata> getPersistentQueries() {
    return Collections.unmodifiableMap(persistentQueries);
  }

  MutableMetaStore getMetaStore() {
    return metaStore;
  }

  ServiceContext getServiceContext() {
    return serviceContext;
  }

  List<ParsedStatement> parse(final String sql) {
    return parser.parse(sql);
  }

  PreparedStatement<?> prepare(final ParsedStatement stmt) {
    try {
      final PreparedStatement<?> preparedStatement = parser.prepare(stmt, metaStore);
      return PreparedStatement.of(
          preparedStatement.getStatementText(),
          AstSanitizer.sanitize(preparedStatement.getStatement(), metaStore)
      );
    } catch (final KaypherStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KaypherStatementException(
          "Exception while preparing statement: " + e.getMessage(), stmt.getStatementText(), e);
    }
  }

  QueryEngine createQueryEngine(final ServiceContext serviceContext) {
    return new QueryEngine(
        serviceContext,
        processingLogContext,
        queryIdGenerator);
  }

  QueryExecutor createQueryExecutor(
      final KaypherConfig kaypherConfig,
      final Map<String, Object> overriddenProperties,
      final ServiceContext serviceContext) {
    return new QueryExecutor(
        kaypherConfig.cloneWithPropertyOverwrite(overriddenProperties),
        overriddenProperties,
        processingLogContext,
        serviceContext,
        metaStore,
        this::unregisterQuery
    );
  }

  DdlCommand createDdlCommand(
      final String sqlExpression,
      final ExecutableDdlStatement statement,
      final KaypherConfig kaypherConfig,
      final Map<String, Object> overriddenProperties
  ) {
    return ddlCommandFactory.create(
        sqlExpression,
        statement,
        kaypherConfig,
        overriddenProperties
    );
  }

  String executeDdl(
      final String sqlExpression,
      final DdlCommand command,
      final Optional<KeyField> keyField) {
    final DdlCommandResult result = ddlCommandExec.execute(command, keyField);
    if (!result.isSuccess()) {
      throw new KaypherStatementException(result.getMessage(), sqlExpression);
    }
    return result.getMessage();
  }

  void registerQuery(final QueryMetadata query) {
    if (query instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
      final QueryId queryId = persistentQuery.getQueryId();

      if (persistentQueries.putIfAbsent(queryId, persistentQuery) != null) {
        throw new IllegalStateException("Query already registered:" + queryId);
      }

      metaStore.updateForPersistentQuery(
          queryId.getId(),
          persistentQuery.getSourceNames(),
          ImmutableSet.of(persistentQuery.getSinkName()));
    }
  }

  private void unregisterQuery(final QueryMetadata query) {
    if (query instanceof PersistentQueryMetadata) {
      final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata) query;
      persistentQueries.remove(persistentQuery.getQueryId());
      metaStore.removePersistentQuery(persistentQuery.getQueryId().getId());
    }

    outerOnQueryCloseCallback.accept(serviceContext, query);
  }
}
