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

package com.treutec.kaypher.function.udaf;

import com.treutec.kaypher.function.BaseAggregateFunction;
import java.util.Collections;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;

public abstract class BaseNumberKudaf<T extends Number> extends BaseAggregateFunction<T, T, T> {

  private final BiFunction<T, T, T> aggregatePrimitive;

  public BaseNumberKudaf(
      final String functionName,
      final Integer argIndexInValue,
      final Schema outputSchema,
      final BiFunction<T, T, T> aggregatePrimitive,
      final String description
  ) {
    super(functionName,
        argIndexInValue,
        () -> null,
        outputSchema,
        outputSchema,
        Collections.singletonList(outputSchema),
        description);
    this.aggregatePrimitive = Objects.requireNonNull(aggregatePrimitive, "aggregatePrimitive");
  }

  @Override
  public final T aggregate(final T currentValue, final T aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }

    if (aggregateValue == null) {
      return currentValue;
    }

    return aggregatePrimitive.apply(aggregateValue, currentValue);
  }

  @Override
  public final Merger<Struct, T> getMerger() {
    return (key, a, b) -> aggregate(a, b);
  }

  @Override
  public Function<T, T> getResultMapper() {
    return Function.identity();
  }
}
