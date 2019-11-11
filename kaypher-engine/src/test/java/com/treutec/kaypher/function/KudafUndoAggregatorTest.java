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
package com.treutec.kaypher.function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.execution.function.TableAggregationFunction;
import com.treutec.kaypher.execution.function.udaf.KudafUndoAggregator;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

public class KudafUndoAggregatorTest {
  private static final InternalFunctionRegistry FUNCTION_REGISTRY = new InternalFunctionRegistry();
  private static final KaypherAggregateFunction SUM_INFO = FUNCTION_REGISTRY.getAggregateFunction(
      "SUM",
      Schema.OPTIONAL_INT32_SCHEMA,
      new AggregateFunctionInitArguments(2)
  );

  private KudafUndoAggregator aggregator;

  @Before
  public void init() {
    final List<TableAggregationFunction<?, ?, ?>> functions =
        ImmutableList.of((TableAggregationFunction)SUM_INFO);
    aggregator = new KudafUndoAggregator(2, functions);
  }

  @Test
  public void shouldApplyUndoableAggregateFunctions() {
    // Given:
    final GenericRow row = new GenericRow(Arrays.asList("snow", "jon", 3));
    final GenericRow aggRow = new GenericRow(Arrays.asList("snow", "jon", 5));

    // When:
    final GenericRow resultRow = aggregator.apply(null, row, aggRow);

    // Then:
    assertThat(resultRow, equalTo(new GenericRow(Arrays.asList("snow", "jon", 2))));
  }
}
