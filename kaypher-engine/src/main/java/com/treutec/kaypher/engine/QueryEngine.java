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

import com.treutec.kaypher.analyzer.AggregateAnalysisResult;
import com.treutec.kaypher.analyzer.Analysis;
import com.treutec.kaypher.analyzer.QueryAnalyzer;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.parser.tree.Query;
import com.treutec.kaypher.parser.tree.Sink;
import com.treutec.kaypher.physical.PhysicalPlan;
import com.treutec.kaypher.physical.PhysicalPlanBuilder;
import com.treutec.kaypher.planner.LogicalPlanNode;
import com.treutec.kaypher.planner.LogicalPlanner;
import com.treutec.kaypher.planner.plan.OutputNode;
import com.treutec.kaypher.query.id.QueryIdGenerator;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.SerdeOptions;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
class QueryEngine {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(QueryEngine.class);

  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final QueryIdGenerator queryIdGenerator;

  QueryEngine(
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final QueryIdGenerator queryIdGenerator
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext"
    );
    this.queryIdGenerator = Objects.requireNonNull(queryIdGenerator, "queryIdGenerator");
  }

  static OutputNode buildQueryLogicalPlan(
      final Query query,
      final Optional<Sink> sink,
      final MetaStore metaStore,
      final KaypherConfig config
  ) {
    final String outputPrefix = config.getString(KaypherConfig.KAYPHER_OUTPUT_TOPIC_NAME_PREFIX_CONFIG);

    final Set<SerdeOption> defaultSerdeOptions = SerdeOptions.buildDefaults(config);

    final QueryAnalyzer queryAnalyzer =
        new QueryAnalyzer(metaStore, outputPrefix, defaultSerdeOptions);

    final Analysis analysis = queryAnalyzer.analyze(query, sink);
    final AggregateAnalysisResult aggAnalysis = queryAnalyzer.analyzeAggregate(query, analysis);

    return new LogicalPlanner(config, analysis, aggAnalysis, metaStore).buildPlan();
  }

  PhysicalPlan<?> buildPhysicalPlan(
      final LogicalPlanNode logicalPlanNode,
      final KaypherConfig kaypherConfig,
      final Map<String, Object> overriddenProperties,
      final MutableMetaStore metaStore
  ) {

    final StreamsBuilder builder = new StreamsBuilder();

    // Build a physical plan, in this case a Kafka Streams DSL
    final PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(
        builder,
        kaypherConfig.cloneWithPropertyOverwrite(overriddenProperties),
        serviceContext,
        processingLogContext,
        metaStore,
        queryIdGenerator
    );

    return physicalPlanBuilder.buildPhysicalPlan(logicalPlanNode);
  }
}
