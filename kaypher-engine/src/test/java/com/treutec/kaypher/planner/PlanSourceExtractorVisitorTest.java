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
package com.treutec.kaypher.planner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.planner.plan.PlanNode;
import com.treutec.kaypher.testutils.AnalysisTestUtil;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.MetaStoreFixture;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.common.utils.Utils;
import org.junit.Before;
import org.junit.Test;

public class PlanSourceExtractorVisitorTest {

  private MetaStore metaStore;
  private KaypherConfig kaypherConfig;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    kaypherConfig = new KaypherConfig(Collections.emptyMap());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldExtractCorrectSourceForSimpleQuery() {
    final PlanNode planNode = buildLogicalPlan("select col0 from TEST2 EMIT CHANGES limit 5;");
    final PlanSourceExtractorVisitor planSourceExtractorVisitor = new PlanSourceExtractorVisitor();
    planSourceExtractorVisitor.process(planNode, null);
    final Set<SourceName> sourceNames = planSourceExtractorVisitor.getSourceNames();
    assertThat(sourceNames.size(), equalTo(1));
    assertThat(sourceNames, equalTo(Utils.mkSet(SourceName.of("TEST2"))));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldExtractCorrectSourceForJoinQuery() {
    final PlanNode planNode = buildLogicalPlan(
        "SELECT t1.col1, t2.col1, t1.col4, t2.col2 FROM test1 t1 LEFT JOIN "
                          + "test2 t2 ON t1.col1 = t2.col1 EMIT CHANGES;");
    final PlanSourceExtractorVisitor planSourceExtractorVisitor = new PlanSourceExtractorVisitor();
    planSourceExtractorVisitor.process(planNode, null);
    final Set<SourceName> sourceNames = planSourceExtractorVisitor.getSourceNames();
    assertThat(sourceNames, equalTo(Utils.mkSet(SourceName.of("TEST1"), SourceName.of("TEST2"))));
  }

  private PlanNode buildLogicalPlan(final String query) {
    return AnalysisTestUtil.buildLogicalPlan(kaypherConfig, query, metaStore);
  }
}
