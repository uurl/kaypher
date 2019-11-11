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

import com.treutec.kaypher.function.udf.UdfMetadata;
import com.treutec.kaypher.util.KaypherException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

public class UdafAggregateFunctionFactory extends AggregateFunctionFactory {

  private final UdfIndex<UdafFactoryInvoker> udfIndex;

  UdafAggregateFunctionFactory(
      final UdfMetadata metadata,
      final List<UdafFactoryInvoker> factoryList
  ) {
    super(metadata);
    udfIndex = new UdfIndex<>(metadata.getName());
    factoryList.forEach(udfIndex::addFunction);
  }

  @Override
  public synchronized KaypherAggregateFunction<?, ?, ?> createAggregateFunction(
      final List<Schema> argTypeList,
      final AggregateFunctionInitArguments initArgs
  ) {
    final UdafFactoryInvoker creator = udfIndex.getFunction(argTypeList);
    if (creator == null) {
      throw new KaypherException("There is no aggregate function with name='" + getName()
          + "' that has arguments of type="
          + argTypeList.stream().map(schema -> schema.type().getName())
          .collect(Collectors.joining(",")));
    }
    return creator.createFunction(initArgs);
  }

  @Override
  public synchronized List<List<Schema>> supportedArgs() {
    return udfIndex.values()
        .stream()
        .map(UdafFactoryInvoker::getArguments)
        .collect(Collectors.toList());
  }
}
