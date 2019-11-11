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

import com.treutec.kaypher.name.FunctionName;
import com.treutec.kaypher.schema.kaypher.SchemaConverters;
import com.treutec.kaypher.schema.kaypher.SchemaConverters.ConnectToSqlTypeConverter;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.util.KaypherException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public abstract class BaseAggregateFunction<I, A, O> implements KaypherAggregateFunction<I, A, O> {

  private static final ConnectToSqlTypeConverter CONNECT_TO_SQL_CONVERTER
      = SchemaConverters.connectToSqlConverter();

  /** An index of the function argument in the row that is used for computing the aggregate.
   * For instance, in the example SELECT key, SUM(ROWTIME), aggregation will be done on a row
   * with two columns (key, ROWTIME) and the `argIndexInValue` will be 1.
   **/
  private final int argIndexInValue;
  private final Supplier<A> initialValueSupplier;
  private final Schema aggregateSchema;
  private final SqlType aggregateType;
  private final Schema outputSchema;
  private final SqlType outputType;
  private final List<Schema> arguments;

  protected final String functionName;
  private final String description;

  public BaseAggregateFunction(
      final String functionName,
      final int argIndexInValue,
      final Supplier<A> initialValueSupplier,
      final Schema aggregateType,
      final Schema outputType,
      final List<Schema> arguments,
      final String description
  ) {
    this.argIndexInValue = argIndexInValue;
    this.initialValueSupplier = () -> {
      final A val = initialValueSupplier.get();
      if (val instanceof Struct && !((Struct) val).schema().isOptional()) {
        throw new KaypherException("Initialize function for " + functionName
            + " must return struct with optional schema");
      }
      return val;
    };
    this.aggregateSchema = Objects.requireNonNull(aggregateType, "aggregateType");
    this.aggregateType = CONNECT_TO_SQL_CONVERTER.toSqlType(aggregateType);
    this.outputSchema = Objects.requireNonNull(outputType, "outputType");
    this.outputType = CONNECT_TO_SQL_CONVERTER.toSqlType(outputType);
    this.arguments = Objects.requireNonNull(arguments, "arguments");
    this.functionName = Objects.requireNonNull(functionName, "functionName");
    this.description = Objects.requireNonNull(description, "description");

    if (!outputType.isOptional() || !aggregateType.isOptional()) {
      throw new IllegalArgumentException("KAYPHER only supports optional field types");
    }
  }

  public FunctionName getFunctionName() {
    return FunctionName.of(functionName);
  }

  public Supplier<A> getInitialValueSupplier() {
    return initialValueSupplier;
  }

  public int getArgIndexInValue() {
    return argIndexInValue;
  }

  public Schema getAggregateType() {
    return aggregateSchema;
  }

  @Override
  public SqlType aggregateType() {
    return aggregateType;
  }

  public Schema getReturnType() {
    return outputSchema;
  }

  @Override
  public SqlType returnType() {
    return outputType;
  }

  public List<Schema> getArguments() {
    return arguments;
  }

  @Override
  public String getDescription() {
    return description;
  }
}
