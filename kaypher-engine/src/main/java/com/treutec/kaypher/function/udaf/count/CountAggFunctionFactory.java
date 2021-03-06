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

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.function.AggregateFunctionFactory;
import com.treutec.kaypher.function.AggregateFunctionInitArguments;
import com.treutec.kaypher.function.KaypherAggregateFunction;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public class CountAggFunctionFactory extends AggregateFunctionFactory {

  private static final String FUNCTION_NAME = "COUNT";

  public CountAggFunctionFactory() {
    super(FUNCTION_NAME);
  }

  @Override
  public KaypherAggregateFunction createAggregateFunction(
      final List<Schema> argTypeList,
      final AggregateFunctionInitArguments initArgs
  ) {
    return new CountKudaf(FUNCTION_NAME, initArgs.udafIndex());
  }

  @Override
  public List<List<Schema>> supportedArgs() {
    // anything is a supported type
    return ImmutableList.of(ImmutableList.of());
  }
}
