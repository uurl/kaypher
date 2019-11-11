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

import com.treutec.kaypher.execution.function.TableAggregationFunction;
import com.treutec.kaypher.function.BaseAggregateFunction;
import com.treutec.kaypher.util.DecimalUtil;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collections;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;

public class DecimalSumKudaf
    extends BaseAggregateFunction<BigDecimal, BigDecimal, BigDecimal>
    implements TableAggregationFunction<BigDecimal, BigDecimal, BigDecimal> {

  private final MathContext context;

  DecimalSumKudaf(
      final String functionName,
      final int argIndexInValue,
      final Schema outputSchema
  ) {
    super(
        functionName,
        argIndexInValue,
        DecimalSumKudaf::initialValue,
        outputSchema,
        outputSchema,
        Collections.singletonList(outputSchema),
        "Computes the sum of decimal values for a key, resulting in a decimal with the same "
            + "precision and scale.");
    context = new MathContext(DecimalUtil.precision(outputSchema));
  }

  @Override
  public BigDecimal aggregate(final BigDecimal currentValue, final BigDecimal aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }

    return aggregateValue.add(currentValue, context);
  }

  @Override
  public Merger<Struct, BigDecimal> getMerger() {
    return (key, agg1, agg2) -> agg1.add(agg2, context);
  }

  @Override
  public Function<BigDecimal, BigDecimal> getResultMapper() {
    return Function.identity();
  }

  @Override
  public BigDecimal undo(final BigDecimal valueToUndo, final BigDecimal aggregateValue) {
    if (valueToUndo == null) {
      return aggregateValue;
    }
    return aggregateValue.subtract(valueToUndo, context);
  }

  private static BigDecimal initialValue() {
    return BigDecimal.ZERO;
  }
}
