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

import com.treutec.kaypher.function.AggregateFunctionFactory;
import com.treutec.kaypher.function.AggregateFunctionInitArguments;
import com.treutec.kaypher.function.KaypherAggregateFunction;
import com.treutec.kaypher.util.DecimalUtil;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.KaypherPreconditions;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public class MaxAggFunctionFactory extends AggregateFunctionFactory {

  private static final String FUNCTION_NAME = "MAX";

  public MaxAggFunctionFactory() {
    super(FUNCTION_NAME);
  }

  @Override
  public KaypherAggregateFunction createAggregateFunction(
      final List<Schema> argTypeList,
      final AggregateFunctionInitArguments initArgs
  ) {
    KaypherPreconditions.checkArgument(
        argTypeList.size() == 1,
        "expected exactly one argument to aggregate MAX function");

    final Schema argSchema = argTypeList.get(0);
    switch (argSchema.type()) {
      case INT32:
        return new IntegerMaxKudaf(FUNCTION_NAME, initArgs.udafIndex());
      case INT64:
        return new LongMaxKudaf(FUNCTION_NAME, initArgs.udafIndex());
      case FLOAT64:
        return new DoubleMaxKudaf(FUNCTION_NAME, initArgs.udafIndex());
      case BYTES:
        DecimalUtil.requireDecimal(argSchema);
        return new DecimalMaxKudaf(FUNCTION_NAME, initArgs.udafIndex(), argSchema);
      default:
        throw new KaypherException("No Max aggregate function with " + argTypeList.get(0) + " "
            + " argument type exists!");

    }
  }

  @Override
  public List<List<Schema>> supportedArgs() {
    return NUMERICAL_ARGS;
  }
}
