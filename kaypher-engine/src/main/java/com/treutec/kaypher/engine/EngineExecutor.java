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

import static com.treutec.kaypher.metastore.model.DataSource.DataSourceType;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import com.treutec.kaypher.KaypherExecutionContext.ExecuteResult;
import com.treutec.kaypher.execution.ddl.commands.CreateSourceCommand;
import com.treutec.kaypher.execution.ddl.commands.CreateStreamCommand;
import com.treutec.kaypher.execution.ddl.commands.CreateTableCommand;
import com.treutec.kaypher.execution.ddl.commands.DdlCommand;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.tree.CreateStreamAsSelect;
import com.treutec.kaypher.parser.tree.CreateTableAsSelect;
import com.treutec.kaypher.parser.tree.ExecutableDdlStatement;
import com.treutec.kaypher.parser.tree.Query;
import com.treutec.kaypher.parser.tree.QueryContainer;
import com.treutec.kaypher.parser.tree.Sink;
import com.treutec.kaypher.physical.PhysicalPlan;
import com.treutec.kaypher.planner.LogicalPlanNode;
import com.treutec.kaypher.planner.PlanSourceExtractorVisitor;
import com.treutec.kaypher.planner.plan.KaypherStructuredDataOutputNode;
import com.treutec.kaypher.planner.plan.OutputNode;
import com.treutec.kaypher.planner.plan.PlanNode;
import com.treutec.kaypher.query.QueryExecutor;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.util.AvroUtil;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.KaypherStatementException;
import com.treutec.kaypher.util.PersistentQueryMetadata;
import com.treutec.kaypher.util.TransientQueryMetadata;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Executor of {@code PreparedStatement} within a specific {@code EngineContext} and using a
 * specific set of config.
 * </p>
 * All statements are executed using a {@code ServiceContext} specified in the constructor. This
 * {@code ServiceContext} might have been initialized with limited permissions to access Kafka
 * resources. The {@code EngineContext} has an internal {@code ServiceContext} that might have more
 * or less permissions than the one specified. This approach is useful when KAYPHER needs to
 * impersonate the current REST user executing the statements.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
final class EngineExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private final EngineContext engineContext;
  private final ServiceContext serviceContext;
  private final KaypherConfig kaypherConfig;
  private final Map<String, Object> overriddenProperties;

  private EngineExecutor(
      final EngineContext engineContext,
      final ServiceContext serviceContext,
      final KaypherConfig kaypherConfig,
      final Map<String, Object> overriddenProperties
  ) {
    this.engineContext = Objects.requireNonNull(engineContext, "engineContext");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.kaypherConfig = Objects.requireNonNull(kaypherConfig, "kaypherConfig");
    this.overriddenProperties =
        Objects.requireNonNull(overriddenProperties, "overriddenProperties");

    KaypherEngineProps.throwOnImmutableOverride(overriddenProperties);
  }

  static EngineExecutor create(
      final EngineContext engineContext,
      final ServiceContext serviceContext,
      final KaypherConfig kaypherConfig,
      final Map<String, Object> overriddenProperties
  ) {
    return new EngineExecutor(engineContext, serviceContext, kaypherConfig, overriddenProperties);
  }

  ExecuteResult execute(final ConfiguredStatement<?> statement) {
    if (statement.getStatement() instanceof Query) {
      return ExecuteResult.of(executeQuery(statement.cast()));
    }
    return execute(plan(statement));
  }

  ExecuteResult execute(final KaypherPlan plan) {
    final Optional<String> ddlResult = plan.getDdlCommand().map(ddl -> executeDdl(plan));
    final Optional<PersistentQueryMetadata> queryMetadata =
        plan.getQueryPlan().map(qp -> executePersistentQuery(plan));
    return queryMetadata.map(ExecuteResult::of).orElseGet(() -> ExecuteResult.of(ddlResult.get()));
  }

  TransientQueryMetadata executeQuery(final ConfiguredStatement<Query> statement) {
    final ExecutorPlans plans = planQuery(statement, statement.getStatement(), Optional.empty());
    final OutputNode outputNode = plans.logicalPlan.getNode().get();
    final QueryExecutor executor = engineContext.createQueryExecutor(
        kaypherConfig,
        overriddenProperties,
        serviceContext
    );
    return executor.buildTransientQuery(
        statement.getStatementText(),
        plans.physicalPlan.getQueryId(),
        getSourceNames(outputNode),
        plans.physicalPlan.getPhysicalPlan(),
        plans.physicalPlan.getPlanSummary(),
        outputNode.getSchema(),
        outputNode.getLimit()
    );
  }

  KaypherPlan plan(final ConfiguredStatement<?> statement) {
    try {
      throwOnNonExecutableStatement(statement);
      if (statement.getStatement() instanceof ExecutableDdlStatement) {
        final DdlCommand ddlCommand = engineContext.createDdlCommand(
            statement.getStatementText(),
            (ExecutableDdlStatement) statement.getStatement(),
            kaypherConfig,
            overriddenProperties
        );
        return new KaypherPlan(
            statement.getStatementText(),
            Optional.of(ddlCommand),
            Optional.empty()
        );
      }
      final ExecutorPlans plans = planQuery(
          statement,
          ((QueryContainer) statement.getStatement()).getQuery(),
          Optional.of(((QueryContainer) statement.getStatement()).getSink())
      );
      final KaypherStructuredDataOutputNode outputNode =
          (KaypherStructuredDataOutputNode) plans.logicalPlan.getNode().get();
      final Optional<DdlCommand> ddlCommand = maybeCreateSinkDdl(
          statement.getStatementText(),
          outputNode,
          plans.physicalPlan.getKeyField());
      validateQuery(outputNode.getNodeOutputType(), statement);
      return new KaypherPlan(
          statement.getStatementText(),
          ddlCommand,
          Optional.of(
              new QueryPlan(
                  getSourceNames(outputNode),
                  outputNode.getIntoSourceName(),
                  plans.physicalPlan
              )
          )
      );
    } catch (final KaypherStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KaypherStatementException(e.getMessage(), statement.getStatementText(), e);
    }
  }

  private ExecutorPlans planQuery(
      final ConfiguredStatement<?> statement,
      final Query query,
      final Optional<Sink> sink) {
    final QueryEngine queryEngine = engineContext.createQueryEngine(serviceContext);
    final OutputNode outputNode = QueryEngine.buildQueryLogicalPlan(
        query,
        sink,
        engineContext.getMetaStore(),
        kaypherConfig.cloneWithPropertyOverwrite(overriddenProperties)
    );
    final LogicalPlanNode logicalPlan = new LogicalPlanNode(
        statement.getStatementText(),
        Optional.of(outputNode)
    );
    final PhysicalPlan<?> physicalPlan = queryEngine.buildPhysicalPlan(
        logicalPlan,
        kaypherConfig,
        overriddenProperties,
        engineContext.getMetaStore()
    );
    return new ExecutorPlans(logicalPlan, physicalPlan);
  }

  private static final class ExecutorPlans {
    private final LogicalPlanNode logicalPlan;
    private final PhysicalPlan<?> physicalPlan;

    private ExecutorPlans(
        final LogicalPlanNode logicalPlan,
        final PhysicalPlan<?> physicalPlan) {
      this.logicalPlan = Objects.requireNonNull(logicalPlan, "logicalPlan");
      this.physicalPlan = Objects.requireNonNull(physicalPlan, "physicalPlanNode");
    }
  }

  private Optional<DdlCommand> maybeCreateSinkDdl(
      final String sql,
      final KaypherStructuredDataOutputNode outputNode,
      final KeyField keyField) {
    if (!outputNode.isDoCreateInto()) {
      validateExistingSink(outputNode, keyField);
      return Optional.empty();
    }
    final CreateSourceCommand ddl;
    if (outputNode.getNodeOutputType() == DataSourceType.KSTREAM) {
      ddl = new CreateStreamCommand(
          sql,
          outputNode.getIntoSourceName(),
          outputNode.getSchema(),
          keyField.ref().map(ColumnRef::name),
          outputNode.getTimestampExtractionPolicy(),
          outputNode.getSerdeOptions(),
          outputNode.getKaypherTopic()
      );
    } else {
      ddl = new CreateTableCommand(
          sql,
          outputNode.getIntoSourceName(),
          outputNode.getSchema(),
          keyField.ref().map(ColumnRef::name),
          outputNode.getTimestampExtractionPolicy(),
          outputNode.getSerdeOptions(),
          outputNode.getKaypherTopic()
      );
    }
    final SchemaRegistryClient srClient = serviceContext.getSchemaRegistryClient();
    AvroUtil.throwOnInvalidSchemaEvolution(sql, ddl, srClient, kaypherConfig);
    return Optional.of(ddl);
  }

  private void validateExistingSink(
      final KaypherStructuredDataOutputNode outputNode,
      final KeyField keyField
  ) {
    final SourceName name = outputNode.getIntoSourceName();
    final DataSource<?> existing = engineContext.getMetaStore().getSource(name);

    if (existing == null) {
      throw new KaypherException(String.format("%s does not exist.", outputNode));
    }

    if (existing.getDataSourceType() != outputNode.getNodeOutputType()) {
      throw new KaypherException(String.format("Incompatible data sink and query result. Data sink"
              + " (%s) type is %s but select query result is %s.",
          name.name(),
          existing.getDataSourceType(),
          outputNode.getNodeOutputType())
      );
    }

    final LogicalSchema resultSchema = outputNode.getSchema();
    final LogicalSchema existingSchema = existing.getSchema();

    if (!resultSchema.equals(existingSchema)) {
      throw new KaypherException("Incompatible schema between results and sink. "
          + "Result schema is " + resultSchema
          + ", but the sink schema is " + existingSchema + ".");
    }

    enforceKeyEquivalence(
        existing.getKeyField().resolve(existingSchema),
        keyField.resolve(resultSchema)
    );
  }

  private static void enforceKeyEquivalence(
      final Optional<Column> sinkKeyCol,
      final Optional<Column> resultKeyCol
  ) {
    if (!sinkKeyCol.isPresent() && !resultKeyCol.isPresent()) {
      return;
    }

    if (sinkKeyCol.isPresent()
        && resultKeyCol.isPresent()
        && sinkKeyCol.get().name().equals(resultKeyCol.get().name())
        && Objects.equals(sinkKeyCol.get().type(), resultKeyCol.get().type())) {
      return;
    }

    throwIncompatibleKeysException(sinkKeyCol, resultKeyCol);
  }

  private static void throwIncompatibleKeysException(
      final Optional<Column> sinkKeyCol,
      final Optional<Column> resultKeyCol
  ) {
    throw new KaypherException(String.format(
        "Incompatible key fields for sink and results. Sink"
            + " key field is %s (type: %s) while result key "
            + "field is %s (type: %s)",
        sinkKeyCol.map(c -> c.name().name()).orElse(null),
        sinkKeyCol.map(Column::type).orElse(null),
        resultKeyCol.map(c -> c.name().name()).orElse(null),
        resultKeyCol.map(Column::type).orElse(null)));
  }

  private static void validateQuery(
      final DataSourceType dataSourceType,
      final ConfiguredStatement<?> statement
  ) {
    if (statement.getStatement() instanceof CreateStreamAsSelect
        && dataSourceType == DataSourceType.KTABLE) {
      throw new KaypherStatementException("Invalid result type. "
          + "Your SELECT query produces a TABLE. "
          + "Please use CREATE TABLE AS SELECT statement instead.",
          statement.getStatementText());
    }

    if (statement.getStatement() instanceof CreateTableAsSelect
        && dataSourceType == DataSourceType.KSTREAM) {
      throw new KaypherStatementException("Invalid result type. "
          + "Your SELECT query produces a STREAM. "
          + "Please use CREATE STREAM AS SELECT statement instead.",
          statement.getStatementText());
    }
  }

  private static void throwOnNonExecutableStatement(final ConfiguredStatement<?> statement) {
    if (!KaypherEngine.isExecutableStatement(statement.getStatement())) {
      throw new KaypherStatementException("Statement not executable", statement.getStatementText());
    }
  }

  private static Set<SourceName> getSourceNames(final PlanNode outputNode) {
    final PlanSourceExtractorVisitor<?, ?> visitor = new PlanSourceExtractorVisitor<>();
    visitor.process(outputNode, null);
    return visitor.getSourceNames();
  }

  private String executeDdl(final KaypherPlan kaypherPlan) {
    final DdlCommand ddlCommand = kaypherPlan.getDdlCommand().get();
    final Optional<KeyField> keyField = kaypherPlan.getQueryPlan()
        .map(QueryPlan::getPhysicalPlan)
        .map(PhysicalPlan::getKeyField);
    try {
      return engineContext.executeDdl(kaypherPlan.getStatementText(), ddlCommand, keyField);
    } catch (final KaypherStatementException e) {
      throw e;
    } catch (final Exception e) {
      throw new KaypherStatementException(e.getMessage(), kaypherPlan.getStatementText(), e);
    }
  }

  private PersistentQueryMetadata executePersistentQuery(final KaypherPlan kaypherPlan) {
    final QueryPlan queryPlan = kaypherPlan.getQueryPlan().get();
    final PhysicalPlan<?> physicalPlan = queryPlan.getPhysicalPlan();
    final QueryExecutor executor = engineContext.createQueryExecutor(
        kaypherConfig,
        overriddenProperties,
        serviceContext
    );
    final PersistentQueryMetadata queryMetadata = executor.buildQuery(
        kaypherPlan.getStatementText(),
        physicalPlan.getQueryId(),
        engineContext.getMetaStore().getSource(queryPlan.getSink()),
        queryPlan.getSources(),
        physicalPlan.getPhysicalPlan(),
        physicalPlan.getPlanSummary()
    );
    engineContext.registerQuery(queryMetadata);
    return queryMetadata;
  }
}
