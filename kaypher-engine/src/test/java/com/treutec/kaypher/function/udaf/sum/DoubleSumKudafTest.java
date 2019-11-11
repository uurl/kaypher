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
package com.treutec.kaypher.function.udaf.sum;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import com.treutec.kaypher.function.AggregateFunctionInitArguments;
import com.treutec.kaypher.function.KaypherAggregateFunction;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;

public class DoubleSumKudafTest extends BaseSumKudafTest<Double, DoubleSumKudaf> {
  protected TGenerator<Double> getNumberGenerator() {
    return Double::valueOf;
  }

  protected DoubleSumKudaf getSumKudaf() {
    final KaypherAggregateFunction aggregateFunction = new SumAggFunctionFactory()
        .createAggregateFunction(Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
            AggregateFunctionInitArguments.EMPTY_ARGS);
    assertThat(aggregateFunction, instanceOf(DoubleSumKudaf.class));
    return  (DoubleSumKudaf) aggregateFunction;
  }
}
