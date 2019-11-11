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
package com.treutec.kaypher.physical;

import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.planner.LogicalPlanNode;
import com.treutec.kaypher.planner.plan.OutputNode;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.query.id.QueryIdGenerator;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.structured.SchemaKStream;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.Objects;
import org.apache.kafka.streams.StreamsBuilder;

public class PhysicalPlanBuilder {
  private final StreamsBuilder builder;
  private final KaypherConfig kaypherConfig;
  private final ServiceContext serviceContext;
  private final ProcessingLogContext processingLogContext;
  private final FunctionRegistry functionRegistry;
  private final QueryIdGenerator queryIdGenerator;

  public PhysicalPlanBuilder(
      final StreamsBuilder builder,
      final KaypherConfig kaypherConfig,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final QueryIdGenerator queryIdGenerator
  ) {
    this.builder = Objects.requireNonNull(builder, "builder");
    this.kaypherConfig = Objects.requireNonNull(kaypherConfig, "kaypherConfig");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.processingLogContext = Objects.requireNonNull(
        processingLogContext,
        "processingLogContext");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.queryIdGenerator = Objects.requireNonNull(queryIdGenerator, "queryIdGenerator");
  }

  public PhysicalPlan<?> buildPhysicalPlan(final LogicalPlanNode logicalPlanNode) {
    final OutputNode outputNode = logicalPlanNode.getNode()
        .orElseThrow(() -> new IllegalArgumentException("Need an output node to build a plan"));

    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    final KaypherQueryBuilder kaypherQueryBuilder = KaypherQueryBuilder.of(
        builder,
        kaypherConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        queryId
    );

    final SchemaKStream<?> resultStream = outputNode.buildStream(kaypherQueryBuilder);
    return new PhysicalPlan<>(
        queryId,
        resultStream.getSourceStep(),
        resultStream.getExecutionPlan(queryId, ""),
        resultStream.getKeyField()
    );
  }
}

