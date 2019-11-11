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
package com.treutec.kaypher.planner.plan;

public class PlanVisitor<C, R> {

  protected R visitPlan(final PlanNode node, final C context) {
    return null;
  }

  protected R visitFilter(final FilterNode node, final C context) {
    return visitPlan(node, context);
  }

  protected R visitProject(final ProjectNode node, final C context) {
    return visitPlan(node, context);
  }

  protected R visitDataSourceNode(final DataSourceNode node, final C context) {
    return visitPlan(node, context);
  }

  protected R visitAggregate(final AggregateNode node, final C context) {
    return visitPlan(node, context);
  }

  protected R visitJoin(final JoinNode node, final C context) {
    return visitPlan(node, context);
  }

  protected R visitOutput(final OutputNode node, final C context) {
    return visitPlan(node, context);
  }

  protected R visitFlatMap(final FlatMapNode node, final C context) {
    return visitPlan(node, context);
  }

}