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
package com.treutec.kaypher.embedded;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.KaypherExecutionContext;
import com.treutec.kaypher.KaypherExecutionContext.ExecuteResult;
import com.treutec.kaypher.ServiceInfo;
import com.treutec.kaypher.engine.KaypherEngine;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.function.MutableFunctionRegistry;
import com.treutec.kaypher.function.UserFunctionLoader;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.parser.KaypherParser.ParsedStatement;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.tree.SetProperty;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.parser.tree.UnsetProperty;
import com.treutec.kaypher.properties.PropertyOverrider;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.query.id.SequentialQueryIdGenerator;
import com.treutec.kaypher.services.DisabledKaypherClient;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.services.ServiceContextFactory;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.statement.Injector;
import com.treutec.kaypher.statement.Injectors;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.PersistentQueryMetadata;
import com.treutec.kaypher.util.QueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaypherContext implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(KaypherContext.class);

  private final ServiceContext serviceContext;
  private final KaypherConfig kaypherConfig;
  private final KaypherEngine kaypherEngine;
  private final BiFunction<KaypherExecutionContext, ServiceContext, Injector> injectorFactory;

  /**
   * Create a KAYPHER context object with the given properties. A KAYPHER context has it's own metastore
   * valid during the life of the object.
   */
  public static KaypherContext create(
      final KaypherConfig kaypherConfig,
      final ProcessingLogContext processingLogContext
  ) {
    Objects.requireNonNull(kaypherConfig, "kaypherConfig cannot be null.");
    final ServiceContext serviceContext =
        ServiceContextFactory.create(kaypherConfig, DisabledKaypherClient.instance());
    final MutableFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    UserFunctionLoader.newInstance(kaypherConfig, functionRegistry, ".").load();
    final ServiceInfo serviceInfo = ServiceInfo.create(kaypherConfig);
    final KaypherEngine engine = new KaypherEngine(
        serviceContext,
        processingLogContext,
        functionRegistry,
        serviceInfo,
        new SequentialQueryIdGenerator());

    return new KaypherContext(
        serviceContext,
        kaypherConfig,
        engine,
        Injectors.DEFAULT
    );
  }

  @VisibleForTesting
  KaypherContext(
      final ServiceContext serviceContext,
      final KaypherConfig kaypherConfig,
      final KaypherEngine kaypherEngine,
      final BiFunction<KaypherExecutionContext, ServiceContext, Injector> injectorFactory
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.kaypherConfig = Objects.requireNonNull(kaypherConfig, "kaypherConfig");
    this.kaypherEngine = Objects.requireNonNull(kaypherEngine, "kaypherEngine");
    this.injectorFactory = Objects.requireNonNull(injectorFactory, "injectorFactory");
  }

  public ServiceContext getServiceContext() {
    return serviceContext;
  }

  public MetaStore getMetaStore() {
    return kaypherEngine.getMetaStore();
  }

  /**
   * Execute the kaypher statement in this context.
   */
  public List<QueryMetadata> sql(final String sql) {
    return sql(sql, Collections.emptyMap());
  }

  public List<QueryMetadata> sql(final String sql, final Map<String, ?> overriddenProperties) {
    final List<ParsedStatement> statements = kaypherEngine.parse(sql);

    final KaypherExecutionContext sandbox = kaypherEngine.createSandbox(kaypherEngine.getServiceContext());
    final Map<String, Object> validationOverrides = new HashMap<>(overriddenProperties);
    for (ParsedStatement stmt : statements) {
      execute(
          sandbox,
          stmt,
          kaypherConfig,
          validationOverrides,
          injectorFactory.apply(sandbox, sandbox.getServiceContext()));
    }

    final List<QueryMetadata> queries = new ArrayList<>();
    final Injector injector = injectorFactory.apply(kaypherEngine, serviceContext);
    final Map<String, Object> executionOverrides = new HashMap<>(overriddenProperties);
    for (final ParsedStatement parsed : statements) {
      execute(kaypherEngine, parsed, kaypherConfig, executionOverrides, injector)
          .getQuery()
          .ifPresent(queries::add);
    }

    for (final QueryMetadata queryMetadata : queries) {
      if (queryMetadata instanceof PersistentQueryMetadata) {
        queryMetadata.start();
      } else {
        LOG.warn("Ignoring statemenst: {}", sql);
        LOG.warn("Only CREATE statements can run in KAYPHER embedded mode.");
      }
    }

    return queries;
  }

  /**
   * @deprecated use {@link #getPersistentQueries}.
   */
  @Deprecated
  public Set<QueryMetadata> getRunningQueries() {
    return new HashSet<>(kaypherEngine.getPersistentQueries());
  }

  public List<PersistentQueryMetadata> getPersistentQueries() {
    return kaypherEngine.getPersistentQueries();
  }

  public void close() {
    kaypherEngine.close();
    serviceContext.close();
  }

  public void terminateQuery(final QueryId queryId) {
    kaypherEngine.getPersistentQuery(queryId).ifPresent(QueryMetadata::close);
  }

  private static ExecuteResult execute(
      final KaypherExecutionContext executionContext,
      final ParsedStatement stmt,
      final KaypherConfig kaypherConfig,
      final Map<String, Object> mutableSessionPropertyOverrides,
      final Injector injector
  ) {
    final PreparedStatement<?> prepared = executionContext.prepare(stmt);

    final ConfiguredStatement<?> configured = injector.inject(ConfiguredStatement.of(
        prepared,
        mutableSessionPropertyOverrides,
        kaypherConfig
    ));

    final CustomExecutor executor =
        CustomExecutors.EXECUTOR_MAP.getOrDefault(
            configured.getStatement().getClass(),
            (serviceContext, s, props) -> executionContext.execute(serviceContext, s));

    return executor.apply(
        executionContext.getServiceContext(),
        configured,
        mutableSessionPropertyOverrides
    );
  }

  @FunctionalInterface
  private interface CustomExecutor {
    ExecuteResult apply(
        ServiceContext serviceContext,
        ConfiguredStatement<?> statement,
        Map<String, Object> mutableSessionPropertyOverrides
    );
  }

  @SuppressWarnings("unchecked")
  private enum CustomExecutors {

    SET_PROPERTY(SetProperty.class, (serviceContext, stmt, props) -> {
      PropertyOverrider.set((ConfiguredStatement<SetProperty>) stmt, props);
      return ExecuteResult.of("Successfully executed " + stmt.getStatement());
    }),
    UNSET_PROPERTY(UnsetProperty.class, (serviceContext, stmt, props) -> {
      PropertyOverrider.unset((ConfiguredStatement<UnsetProperty>) stmt, props);
      return ExecuteResult.of("Successfully executed " + stmt.getStatement());
    })
    ;

    public static final Map<Class<? extends Statement>, CustomExecutor> EXECUTOR_MAP =
        ImmutableMap.copyOf(
            EnumSet.allOf(CustomExecutors.class)
                .stream()
                .collect(Collectors.toMap(
                    CustomExecutors::getStatementClass,
                    CustomExecutors::getExecutor))
        );

    private final Class<? extends Statement> statementClass;
    private final CustomExecutor executor;

    CustomExecutors(
        final Class<? extends Statement> statementClass,
        final CustomExecutor executor) {
      this.statementClass = Objects.requireNonNull(statementClass, "statementClass");
      this.executor = Objects.requireNonNull(executor, "executor");
    }

    private Class<? extends Statement> getStatementClass() {
      return statementClass;
    }

    private CustomExecutor getExecutor() {
      return this::execute;
    }

    public ExecuteResult execute(
        final ServiceContext serviceContext,
        final ConfiguredStatement<?> statement,
        final Map<String, Object> mutableSessionPropertyOverrides
    ) {
      return executor.apply(serviceContext, statement, mutableSessionPropertyOverrides);
    }
  }
}
