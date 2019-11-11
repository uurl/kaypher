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
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.kafka.connect.data.Schema;


public abstract class AggregateFunctionFactory {

  private final List<KaypherAggregateFunction<?, ?>> aggregateFunctionList;
  private final UdfMetadata metadata;

  public AggregateFunctionFactory(final String functionName,
                                  final List<KaypherAggregateFunction<?, ?>> aggregateFunctionList) {
    this(new UdfMetadata(functionName, "", "treutec", "", KaypherFunction.INTERNAL_PATH, false),
        aggregateFunctionList);
  }

  public AggregateFunctionFactory(final UdfMetadata metadata,
                                  final List<KaypherAggregateFunction<?, ?>> aggregateFunctionList) {
    this.metadata = Objects.requireNonNull(metadata, "metadata can't be null");
    this.aggregateFunctionList = Objects.requireNonNull(aggregateFunctionList,
        "aggregateFunctionList can't be null");
  }

  public abstract KaypherAggregateFunction<?, ?> getProperAggregateFunction(List<Schema> argTypeList);

  public String getName() {
    return metadata.getName();
  }

  public String getDescription() {
    return metadata.getDescription();
  }

  public String getPath() {
    return metadata.getPath();
  }

  public String getAuthor() {
    return metadata.getAuthor();
  }

  public String getVersion() {
    return metadata.getVersion();
  }

  public void eachFunction(final Consumer<KaypherAggregateFunction<?, ?>> consumer) {
    aggregateFunctionList.forEach(consumer);
  }

  protected List<KaypherAggregateFunction<?, ?>> getAggregateFunctionList() {
    return aggregateFunctionList;
  }

  public boolean isInternal() {
    return metadata.isInternal();
  }
}
