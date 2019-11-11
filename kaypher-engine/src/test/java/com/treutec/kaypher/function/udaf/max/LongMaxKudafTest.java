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

public class LongMaxKudafTest {

  @Test
  public void shouldFindCorrectMax() {
    final LongMaxKudaf longMaxKudaf = getLongMaxKudaf();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    long currentMax = Long.MIN_VALUE;
    for (final long i: values) {
      currentMax = longMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(8L, equalTo(currentMax));
  }

  @Test
  public void shouldHandleNull() {
    final LongMaxKudaf longMaxKudaf = getLongMaxKudaf();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    Long currentMax = null;

    // null before any aggregation
    currentMax = longMaxKudaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final long i: values) {
      currentMax = longMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(8L, equalTo(currentMax));

    // null should not impact result
    currentMax = longMaxKudaf.aggregate(null, currentMax);
    assertThat(8L, equalTo(currentMax));
  }

  @Test
  public void shouldFindCorrectMaxForMerge() {
    final LongMaxKudaf longMaxKudaf = getLongMaxKudaf();
    final Merger<Struct, Long> merger = longMaxKudaf.getMerger();
    final Long mergeResult1 = merger.apply(null, 10L, 12L);
    assertThat(mergeResult1, equalTo(12L));
    final Long mergeResult2 = merger.apply(null, 10L, -12L);
    assertThat(mergeResult2, equalTo(10L));
    final Long mergeResult3 = merger.apply(null, -10L, 0L);
    assertThat(mergeResult3, equalTo(0L));

  }

  private LongMaxKudaf getLongMaxKudaf() {
    final KaypherAggregateFunction aggregateFunction = new MaxAggFunctionFactory()
        .createAggregateFunction(Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA),
            AggregateFunctionInitArguments.EMPTY_ARGS);
    assertThat(aggregateFunction, instanceOf(LongMaxKudaf.class));
    return  (LongMaxKudaf) aggregateFunction;
  }

}
