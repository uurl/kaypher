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
package com.treutec.kaypher.analyzer;

import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.Expression;
import java.util.Map;
import java.util.Set;

interface AggregateAnalysis extends AggregateAnalysisResult {

  /**
   * Get a map of non-aggregate select expression to the set of source schema fields the
   * expression uses.
   *
   * @return the map of select expression to the set of source schema fields.
   */
  Map<Expression, Set<ColumnReferenceExp>> getNonAggregateSelectExpressions();

  /**
   * Get the set of select fields that are involved in aggregate columns, but not as parameters
   * to the aggregate functions.
   *
   * @return the set of fields used in aggregate columns outside of aggregate function parameters.
   */
  Set<ColumnReferenceExp> getAggregateSelectFields();

  /**
   * Get the set of columns from the source schema that are used in the HAVING clause outside
   * of aggregate functions.
   *
   * @return the set of non-aggregate columns used in the HAVING clause.
   */
  Set<ColumnReferenceExp> getNonAggregateHavingFields();
}
