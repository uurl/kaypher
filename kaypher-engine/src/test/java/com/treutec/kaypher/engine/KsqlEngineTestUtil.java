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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import com.treutec.kaypher.KaypherExecutionContext;
import com.treutec.kaypher.KaypherExecutionContext.ExecuteResult;
import com.treutec.kaypher.internal.KaypherEngineMetrics;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.parser.KaypherParser.ParsedStatement;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.query.id.QueryIdGenerator;
import com.treutec.kaypher.query.id.SequentialQueryIdGenerator;
import com.treutec.kaypher.schema.kaypher.inference.DefaultSchemaInjector;
import com.treutec.kaypher.schema.kaypher.inference.SchemaRegistryTopicSchemaSupplier;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherStatementException;
import com.treutec.kaypher.util.QueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class KaypherEngineTestUtil {

  private KaypherEngineTestUtil() {
  }

  public static KaypherEngine createKaypherEngine(
      final ServiceContext serviceContext,
      final MutableMetaStore metaStore
  ) {
    return new KaypherEngine(
        serviceContext,
        ProcessingLogContext.create(),
        "test_instance_",
        metaStore,
        (engine) -> new KaypherEngineMetrics("", engine, Collections.emptyMap(), Optional.empty()),
        new SequentialQueryIdGenerator()
    );
  }

  public static KaypherEngine createKaypherEngine(
      final ServiceContext serviceContext,
      final MutableMetaStore metaStore,
      final Function<KaypherEngine, KaypherEngineMetrics> engineMetricsFactory,
      final QueryIdGenerator queryIdGenerator
  ) {
    return new KaypherEngine(
        serviceContext,
        ProcessingLogContext.create(),
        "test_instance_",
        metaStore,
        engineMetricsFactory,
        queryIdGenerator
    );
  }

  public static List<QueryMetadata> execute(
      final ServiceContext serviceContext,
      final KaypherEngine engine,
      final String sql,
      final KaypherConfig kaypherConfig,
      final Map<String, Object> overriddenProperties
  ) {
    return execute(serviceContext, engine, sql, kaypherConfig, overriddenProperties, Optional.empty());
  }

  /**
   * @param srClient if supplied, then schemas can be inferred from the schema registry.
   */
  public static List<QueryMetadata> execute(
      final ServiceContext serviceContext,
      final KaypherEngine engine,
      final String sql,
      final KaypherConfig kaypherConfig,
      final Map<String, Object> overriddenProperties,
      final Optional<SchemaRegistryClient> srClient
  ) {
    final List<ParsedStatement> statements = engine.parse(sql);

    final Optional<DefaultSchemaInjector> schemaInjector = srClient
        .map(SchemaRegistryTopicSchemaSupplier::new)
        .map(DefaultSchemaInjector::new);

    return statements.stream()
        .map(stmt ->
            execute(serviceContext, engine, stmt, kaypherConfig, overriddenProperties, schemaInjector))
        .map(ExecuteResult::getQuery)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  @SuppressWarnings({"rawtypes","unchecked"})
  private static ExecuteResult execute(
      final ServiceContext serviceContext,
      final KaypherExecutionContext executionContext,
      final ParsedStatement stmt,
      final KaypherConfig kaypherConfig,
      final Map<String, Object> overriddenProperties,
      final Optional<DefaultSchemaInjector> schemaInjector
  ) {
    final PreparedStatement<?> prepared = executionContext.prepare(stmt);
    final ConfiguredStatement<?> configured = ConfiguredStatement.of(
        prepared, overriddenProperties, kaypherConfig);
    final ConfiguredStatement<?> withSchema =
        schemaInjector
            .map(injector -> injector.inject(configured))
            .orElse((ConfiguredStatement) configured);
    final ConfiguredStatement<?> reformatted =
        new SqlFormatInjector(executionContext).inject(withSchema);
    try {
      return executionContext.execute(serviceContext, reformatted);
    } catch (final KaypherStatementException e) {
      // use the original statement text in the exception so that tests
      // can easily check that the failed statement is the input statement
      throw new KaypherStatementException(e.getRawMessage(), stmt.getStatementText(), e.getCause());
    }
  }
}
