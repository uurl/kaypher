/*
 * Copyright 2019 Koneksys
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

package com.koneksys.kaypher.function;

import com.koneksys.kaypher.function.udf.UdfMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;


public class TestFunctionRegistry implements MutableFunctionRegistry {
  private final Map<String, UdfFactory> udfs = new HashMap<>();
  private final Map<String, AggregateFunctionFactory> udafs = new HashMap<>();

  @Override
  public UdfFactory getUdfFactory(final String functionName) {
    return udfs.get(functionName);
  }

  @Override
  public void addFunction(final KsqlFunction kaypherFunction) {
    ensureFunctionFactory(new UdfFactory(
        kaypherFunction.getKudfClass(),
        new UdfMetadata(kaypherFunction.getFunctionName(),
            "",
            "",
            "",
            "", false)));
    final UdfFactory udfFactory = udfs.get(kaypherFunction.getFunctionName());
    udfFactory.addFunction(kaypherFunction);  }

  @Override
  public UdfFactory ensureFunctionFactory(final UdfFactory factory) {
    return udfs.putIfAbsent(factory.getName().toUpperCase(), factory);
  }

  @Override
  public boolean isAggregate(final String functionName) {
    return udafs.containsKey(functionName.toUpperCase());
  }

  @Override
  public KsqlAggregateFunction<?, ?> getAggregate(
      final String functionName,
      final Schema expressionType
  ) {
    return udafs.get(functionName.toUpperCase()).getProperAggregateFunction(
        Collections.singletonList(expressionType));
  }

  @Override
  public void addAggregateFunctionFactory(final AggregateFunctionFactory aggregateFunctionFactory) {
    udafs.put(aggregateFunctionFactory.getName().toUpperCase(), aggregateFunctionFactory);
  }

  @Override
  public List<UdfFactory> listFunctions() {
    return new ArrayList<>(udfs.values());
  }

  @Override
  public AggregateFunctionFactory getAggregateFactory(final String functionName) {
    return udafs.get(functionName.toUpperCase());
  }

  @Override
  public List<AggregateFunctionFactory> listAggregateFunctions() {
    return new ArrayList<>(udafs.values());
  }
}
