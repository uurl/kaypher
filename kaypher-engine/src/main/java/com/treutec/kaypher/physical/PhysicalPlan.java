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

import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.execution.plan.ExecutionStep;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.query.QueryId;
import java.util.Objects;

@Immutable
public final class PhysicalPlan<T> {
  private final QueryId queryId;
  private final ExecutionStep<T> physicalPlan;
  private final String planSummary;
  private final transient KeyField keyField;

  PhysicalPlan(
      final QueryId queryId,
      final ExecutionStep<T> physicalPlan,
      final String planSummary,
      final KeyField keyField
  ) {
    this.queryId = Objects.requireNonNull(queryId, "queryId");
    this.physicalPlan = Objects.requireNonNull(physicalPlan, "physicalPlan");
    this.planSummary = Objects.requireNonNull(planSummary, "planSummary");
    this.keyField = Objects.requireNonNull(keyField, "keyField");
  }

  public ExecutionStep<?> getPhysicalPlan() {
    return physicalPlan;
  }

  public String getPlanSummary() {
    return planSummary;
  }

  public KeyField getKeyField() {
    return keyField;
  }

  public QueryId getQueryId() {
    return queryId;
  }
}
