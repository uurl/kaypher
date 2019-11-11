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
package com.treutec.kaypher.testutils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import com.treutec.kaypher.analyzer.AggregateAnalysisResult;
import com.treutec.kaypher.analyzer.Analysis;
import com.treutec.kaypher.analyzer.QueryAnalyzer;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.KaypherParserTestUtil;
import com.treutec.kaypher.parser.tree.Query;
import com.treutec.kaypher.parser.tree.QueryContainer;
import com.treutec.kaypher.parser.tree.Sink;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.planner.LogicalPlanner;
import com.treutec.kaypher.planner.plan.OutputNode;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.List;
import java.util.Optional;

public final class AnalysisTestUtil {

  private AnalysisTestUtil() {
  }

  public static Analysis analyzeQuery(final String queryStr, final MetaStore metaStore) {
    return new Analyzer(queryStr, metaStore).analysis;
  }

  public static OutputNode buildLogicalPlan(
      final KaypherConfig kaypherConfig,
      final String queryStr,
      final MetaStore metaStore
  ) {
    final Analyzer analyzer = new Analyzer(queryStr, metaStore);

    final LogicalPlanner logicalPlanner = new LogicalPlanner(
        kaypherConfig,
        analyzer.analysis,
        analyzer.aggregateAnalysis(),
        metaStore);

    return logicalPlanner.buildPlan();
  }

  private static class Analyzer {
    private final Query query;
    private final Analysis analysis;
    private final QueryAnalyzer queryAnalyzer;

    private Analyzer(final String queryStr, final MetaStore metaStore) {
      this.queryAnalyzer = new QueryAnalyzer(metaStore, "", SerdeOption.none());
      final Statement statement = parseStatement(queryStr, metaStore);
      this.query = statement instanceof QueryContainer
        ? ((QueryContainer)statement).getQuery()
        : (Query) statement;

      final Optional<Sink> sink = statement instanceof QueryContainer
          ? Optional.of(((QueryContainer)statement).getSink())
          : Optional.empty();

      this.analysis = queryAnalyzer.analyze(query, sink);
    }

    private static Statement parseStatement(
        final String queryStr,
        final MetaStore metaStore
    ) {
      final List<PreparedStatement<?>> statements =
          KaypherParserTestUtil.buildAst(queryStr, metaStore);
      assertThat(statements, hasSize(1));
      return statements.get(0).getStatement();
    }

    AggregateAnalysisResult aggregateAnalysis() {
      return queryAnalyzer.analyzeAggregate(query, analysis);
    }
  }
}
