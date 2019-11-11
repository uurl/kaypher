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

import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.planner.plan.AggregateNode;
import com.treutec.kaypher.planner.plan.DataSourceNode;
import com.treutec.kaypher.planner.plan.FilterNode;
import com.treutec.kaypher.planner.plan.FlatMapNode;
import com.treutec.kaypher.planner.plan.JoinNode;
import com.treutec.kaypher.planner.plan.OutputNode;
import com.treutec.kaypher.planner.plan.PlanNode;
import com.treutec.kaypher.planner.plan.PlanVisitor;
import com.treutec.kaypher.planner.plan.ProjectNode;
import java.util.HashSet;
import java.util.Set;

public class PlanSourceExtractorVisitor<C, R> extends PlanVisitor<C, R> {

  private final Set<SourceName> sourceNames;

  public PlanSourceExtractorVisitor() {
    sourceNames = new HashSet<>();
  }

  public R process(final PlanNode node, final C context) {
    return node.accept(this, context);
  }

  protected R visitPlan(final PlanNode node, final C context) {
    return null;
  }

  protected R visitFilter(final FilterNode node, final C context) {
    process(node.getSources().get(0), context);
    return null;
  }

  protected R visitProject(final ProjectNode node, final C context) {
    return process(node.getSource(), context);
  }

  protected R visitDataSourceNode(final DataSourceNode node, final C context) {
    sourceNames.add(node.getDataSource().getName());
    return null;
  }

  protected R visitJoin(final JoinNode node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);
    return null;
  }

  protected R visitAggregate(final AggregateNode node, final C context) {
    process(node.getSources().get(0), context);
    return null;
  }

  protected R visitOutput(final OutputNode node, final C context) {
    process(node.getSources().get(0), context);
    return null;
  }

  @Override
  protected R visitFlatMap(final FlatMapNode node, final C context) {
    process(node.getSources().get(0), context);
    return null;
  }

  public Set<SourceName> getSourceNames() {
    return sourceNames;
  }
}
