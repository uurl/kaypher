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
package com.treutec.kaypher.function.udaf.max;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import com.treutec.kaypher.function.AggregateFunctionInitArguments;
import com.treutec.kaypher.function.KaypherAggregateFunction;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

public class DoubleMaxKudafTest {

  @Test
  public void shouldFindCorrectMax() {
    final DoubleMaxKudaf doubleMaxKudaf = getDoubleMaxKudaf();
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    Double currentMax = null;
    for (final double i: values) {
      currentMax = doubleMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(8.0, equalTo(currentMax));
  }

  @Test
  public void shouldHandleNull() {
    final DoubleMaxKudaf doubleMaxKudaf = getDoubleMaxKudaf();
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    Double currentMax = null;

    // aggregate null before any aggregation
    currentMax = doubleMaxKudaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final double i: values) {
      currentMax = doubleMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(8.0, equalTo(currentMax));

    // null should not impact result
    currentMax = doubleMaxKudaf.aggregate(null, currentMax);
    assertThat(8.0, equalTo(currentMax));
  }
  @Test
  public void shouldFindCorrectMaxForMerge() {
    final DoubleMaxKudaf doubleMaxKudaf = getDoubleMaxKudaf();
    final Merger<Struct, Double> merger = doubleMaxKudaf.getMerger();
    final Double mergeResult1 = merger.apply(null, 10.0, 12.0);
    assertThat(mergeResult1, equalTo(12.0));
    final Double mergeResult2 = merger.apply(null, 10.0, -12.0);
    assertThat(mergeResult2, equalTo(10.0));
    final Double mergeResult3 = merger.apply(null, -10.0, 0.0);
    assertThat(mergeResult3, equalTo(0.0));

  }

  private DoubleMaxKudaf getDoubleMaxKudaf() {
    final KaypherAggregateFunction aggregateFunction = new MaxAggFunctionFactory()
        .createAggregateFunction(Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
            AggregateFunctionInitArguments.EMPTY_ARGS);
    assertThat(aggregateFunction, instanceOf(DoubleMaxKudaf.class));
    return  (DoubleMaxKudaf) aggregateFunction;
  }

}
