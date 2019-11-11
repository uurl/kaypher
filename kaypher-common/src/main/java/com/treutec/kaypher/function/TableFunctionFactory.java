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
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

public class TableFunctionFactory {

  private final UdfIndex<KaypherTableFunction> udtfIndex;

  private final UdfMetadata metadata;

  public TableFunctionFactory(final UdfMetadata metadata) {
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.udtfIndex = new UdfIndex<>(metadata.getName());
  }

  public UdfMetadata getMetadata() {
    return metadata;
  }

  public String getName() {
    return metadata.getName();
  }

  public synchronized void eachFunction(final Consumer<KaypherTableFunction> consumer) {
    udtfIndex.values().forEach(consumer);
  }

  public synchronized KaypherTableFunction createTableFunction(final List<Schema> argTypeList) {
    return udtfIndex.getFunction(argTypeList);
  }

  protected synchronized List<List<Schema>> supportedArgs() {
    return udtfIndex.values()
        .stream()
        .map(KaypherTableFunction::getArguments)
        .collect(Collectors.toList());
  }

  synchronized void addFunction(final KaypherTableFunction tableFunction) {
    udtfIndex.addFunction(tableFunction);
  }

}
