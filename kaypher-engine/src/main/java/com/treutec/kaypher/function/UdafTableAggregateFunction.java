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

import com.treutec.kaypher.execution.function.TableAggregationFunction;
import com.treutec.kaypher.function.udaf.TableUdaf;
import com.treutec.kaypher.function.udaf.Udaf;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.data.Schema;

public class UdafTableAggregateFunction<I, A, O>
    extends UdafAggregateFunction<I, A, O> implements TableAggregationFunction<I, A, O> {

  public UdafTableAggregateFunction(
      final String functionName,
      final int udafIndex,
      final Udaf<I, A, O> udaf,
      final Schema aggregateType,
      final Schema outputType,
      final List<Schema> arguments,
      final String description,
      final Optional<Metrics> metrics,
      final String method) {
    super(functionName, udafIndex, udaf, aggregateType, outputType, arguments, description,
        metrics, method);
  }

  @Override
  public A undo(final I valueToUndo, final A aggregateValue) {
    return ((TableUdaf<I, A, O>)udaf).undo(valueToUndo, aggregateValue);
  }
}
