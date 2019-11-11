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
package com.treutec.kaypher.function.udaf.count;

import com.treutec.kaypher.execution.function.TableAggregationFunction;
import com.treutec.kaypher.function.BaseAggregateFunction;
import java.util.Collections;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;

public class CountKudaf
    extends BaseAggregateFunction<Object, Long, Long>
    implements TableAggregationFunction<Object, Long, Long> {

  CountKudaf(final String functionName, final int argIndexInValue) {
    super(functionName,
          argIndexInValue, () -> 0L,
          Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
         "Counts records by key."
    );
  }

  @Override
  public Long aggregate(final Object currentValue, final Long aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }
    return aggregateValue + 1;
  }

  @Override
  public Merger<Struct, Long> getMerger() {
    return (aggKey, aggOne, aggTwo) -> aggOne + aggTwo;
  }

  @Override
  public Function<Long, Long> getResultMapper() {
    return Function.identity();
  }


  @Override
  public Long undo(final Object valueToUndo, final Long aggregateValue) {
    if (valueToUndo == null) {
      return aggregateValue;
    }
    return aggregateValue - 1;
  }

}
