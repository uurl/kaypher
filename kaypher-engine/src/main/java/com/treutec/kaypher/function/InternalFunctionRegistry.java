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

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.execution.function.udf.structfieldextractor.FetchFieldFromStruct;
import com.treutec.kaypher.function.udaf.count.CountAggFunctionFactory;
import com.treutec.kaypher.function.udaf.max.MaxAggFunctionFactory;
import com.treutec.kaypher.function.udaf.min.MinAggFunctionFactory;
import com.treutec.kaypher.function.udaf.sum.SumAggFunctionFactory;
import com.treutec.kaypher.function.udaf.topk.TopKAggregateFunctionFactory;
import com.treutec.kaypher.function.udaf.topkdistinct.TopkDistinctAggFunctionFactory;
import com.treutec.kaypher.function.udf.UdfMetadata;
import com.treutec.kaypher.function.udf.json.ArrayContainsKudf;
import com.treutec.kaypher.function.udf.json.JsonExtractStringKudf;
import com.treutec.kaypher.function.udf.math.CeilKudf;
import com.treutec.kaypher.function.udf.math.RandomKudf;
import com.treutec.kaypher.function.udf.string.ConcatKudf;
import com.treutec.kaypher.function.udf.string.IfNullKudf;
import com.treutec.kaypher.function.udf.string.LCaseKudf;
import com.treutec.kaypher.function.udf.string.LenKudf;
import com.treutec.kaypher.function.udf.string.TrimKudf;
import com.treutec.kaypher.function.udf.string.UCaseKudf;
import com.treutec.kaypher.name.FunctionName;
import com.treutec.kaypher.util.KaypherConstants;
import com.treutec.kaypher.util.KaypherException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

@ThreadSafe
public class InternalFunctionRegistry implements MutableFunctionRegistry {

  private final Map<String, UdfFactory> udfs = new HashMap<>();
  private final Map<String, AggregateFunctionFactory> udafs = new HashMap<>();
  private final Map<String, TableFunctionFactory> udtfs = new HashMap<>();
  private final FunctionNameValidator functionNameValidator = new FunctionNameValidator();

  public InternalFunctionRegistry() {
    new BuiltInInitializer(this).init();
  }

  public synchronized UdfFactory getUdfFactory(final String functionName) {
    final UdfFactory udfFactory = udfs.get(functionName.toUpperCase());
    if (udfFactory == null) {
      throw new KaypherException("Can't find any functions with the name '" + functionName + "'");
    }
    return udfFactory;
  }

  @Override
  public synchronized void addFunction(final KaypherScalarFunction kaypherFunction) {
    final UdfFactory udfFactory = udfs.get(kaypherFunction.getFunctionName().name().toUpperCase());
    if (udfFactory == null) {
      throw new KaypherException("Unknown function factory: " + kaypherFunction.getFunctionName());
    }
    udfFactory.addFunction(kaypherFunction);
  }

  @Override
  public synchronized UdfFactory ensureFunctionFactory(final UdfFactory factory) {
    validateFunctionName(factory.getName());

    final String functionName = factory.getName().toUpperCase();
    if (udafs.containsKey(functionName)) {
      throw new KaypherException("UdfFactory already registered as aggregate: " + functionName);
    }
    if (udtfs.containsKey(functionName)) {
      throw new KaypherException("UdfFactory already registered as table function: " + functionName);
    }

    final UdfFactory existing = udfs.putIfAbsent(functionName, factory);
    if (existing != null && !existing.matches(factory)) {
      throw new KaypherException("UdfFactory not compatible with existing factory."
          + " function: " + functionName
          + " existing: " + existing
          + ", factory: " + factory);
    }

    return existing == null ? factory : existing;
  }

  @Override
  public synchronized boolean isAggregate(final String functionName) {
    return udafs.containsKey(functionName.toUpperCase());
  }

  public synchronized boolean isTableFunction(final String functionName) {
    return udtfs.containsKey(functionName.toUpperCase());
  }

  @Override
  public synchronized KaypherAggregateFunction getAggregateFunction(
      final String functionName,
      final Schema argumentType,
      final AggregateFunctionInitArguments initArgs
  ) {
    final AggregateFunctionFactory udafFactory = udafs.get(functionName.toUpperCase());
    if (udafFactory == null) {
      throw new KaypherException("No aggregate function with name " + functionName + " exists!");
    }
    return udafFactory.createAggregateFunction(
        Collections.singletonList(argumentType),
        initArgs
    );
  }

  @Override
  public synchronized KaypherTableFunction getTableFunction(
      final String functionName,
      final List<Schema> argumentTypes
  ) {
    final TableFunctionFactory udtfFactory = udtfs.get(functionName.toUpperCase());
    if (udtfFactory == null) {
      throw new KaypherException("No table function with name " + functionName + " exists!");
    }

    return udtfFactory.createTableFunction(argumentTypes);
  }

  @Override
  public synchronized void addAggregateFunctionFactory(
      final AggregateFunctionFactory aggregateFunctionFactory) {
    final String functionName = aggregateFunctionFactory.getName().toUpperCase();
    validateFunctionName(functionName);

    if (udfs.containsKey(functionName)) {
      throw new KaypherException(
          "Aggregate function already registered as non-aggregate: " + functionName);
    }

    if (udtfs.containsKey(functionName)) {
      throw new KaypherException(
          "Aggregate function already registered as table function: " + functionName);
    }

    if (udafs.putIfAbsent(functionName, aggregateFunctionFactory) != null) {
      throw new KaypherException("Aggregate function already registered: " + functionName);
    }

  }

  @Override
  public synchronized void addTableFunctionFactory(
      final TableFunctionFactory tableFunctionFactory) {
    final String functionName = tableFunctionFactory.getName().toUpperCase();
    validateFunctionName(functionName);

    if (udfs.containsKey(functionName)) {
      throw new KaypherException(
          "Table function already registered as non-aggregate: " + functionName);
    }

    if (udafs.containsKey(functionName)) {
      throw new KaypherException(
          "Table function already registered as aggregate: " + functionName);
    }

    if (udtfs.putIfAbsent(functionName, tableFunctionFactory) != null) {
      throw new KaypherException("Table function already registered: " + functionName);
    }

  }

  @Override
  public synchronized List<UdfFactory> listFunctions() {
    return new ArrayList<>(udfs.values());
  }

  @Override
  public synchronized AggregateFunctionFactory getAggregateFactory(final String functionName) {
    final AggregateFunctionFactory udafFactory = udafs.get(functionName.toUpperCase());
    if (udafFactory == null) {
      throw new KaypherException(
          "Can not find any aggregate functions with the name '" + functionName + "'");
    }

    return udafFactory;
  }

  @Override
  public synchronized TableFunctionFactory getTableFunctionFactory(final String functionName) {
    final TableFunctionFactory tableFunctionFactory = udtfs.get(functionName.toUpperCase());
    if (tableFunctionFactory == null) {
      throw new KaypherException(
          "Can not find any table functions with the name '" + functionName + "'");
    }
    return tableFunctionFactory;
  }

  @Override
  public synchronized List<AggregateFunctionFactory> listAggregateFunctions() {
    return new ArrayList<>(udafs.values());
  }

  @Override
  public synchronized List<TableFunctionFactory> listTableFunctions() {
    return new ArrayList<>(udtfs.values());
  }

  private void validateFunctionName(final String functionName) {
    if (!functionNameValidator.test(functionName)) {
      throw new KaypherException(functionName + " is not a valid function name."
          + " Function names must be valid java identifiers and not a KAYPHER reserved word"
      );
    }
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private static final class BuiltInInitializer {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

    private final InternalFunctionRegistry functionRegistry;

    private BuiltInInitializer(
        final InternalFunctionRegistry functionRegistry
    ) {
      this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    }

    private static UdfFactory builtInUdfFactory(
        final KaypherScalarFunction kaypherFunction,
        final boolean internal
    ) {
      final UdfMetadata metadata = new UdfMetadata(
          kaypherFunction.getFunctionName().name(),
          kaypherFunction.getDescription(),
          KaypherConstants.CONFLUENT_AUTHOR,
          "",
          KaypherScalarFunction.INTERNAL_PATH,
          internal
      );

      return new UdfFactory(kaypherFunction.getKudfClass(), metadata);
    }

    private void init() {
      addStringFunctions();
      addMathFunctions();
      addJsonFunctions();
      addStructFieldFetcher();
      addUdafFunctions();
    }

    private void addStringFunctions() {

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
          FunctionName.of("LCASE"), LCaseKudf.class
      ));

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
          FunctionName.of("UCASE"), UCaseKudf.class
      ));

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
          FunctionName.of(ConcatKudf.NAME), ConcatKudf.class
      ));

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
          FunctionName.of("TRIM"), TrimKudf.class
      ));

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          ImmutableList.of(
              Schema.OPTIONAL_STRING_SCHEMA,
              Schema.OPTIONAL_STRING_SCHEMA
          ),
          FunctionName.of("IFNULL"), IfNullKudf.class
      ));

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_INT32_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
          FunctionName.of("LEN"),
          LenKudf.class
      ));
    }

    private void addMathFunctions() {

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_FLOAT64_SCHEMA,
          Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
          FunctionName.of("CEIL"),
          CeilKudf.class
      ));

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_FLOAT64_SCHEMA,
          Collections.emptyList(),
          FunctionName.of("RANDOM"),
          RandomKudf.class
      ));
    }

    private void addJsonFunctions() {

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_STRING_SCHEMA,
          ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
          JsonExtractStringKudf.FUNCTION_NAME,
          JsonExtractStringKudf.class
      ));

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_BOOLEAN_SCHEMA,
          ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA),
          FunctionName.of("ARRAYCONTAINS"),
          ArrayContainsKudf.class
      ));

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_BOOLEAN_SCHEMA,
          ImmutableList.of(
              SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
              Schema.OPTIONAL_STRING_SCHEMA
          ),
          FunctionName.of("ARRAYCONTAINS"),
          ArrayContainsKudf.class
      ));

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_BOOLEAN_SCHEMA,
          ImmutableList.of(
              SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(),
              Schema.OPTIONAL_INT32_SCHEMA
          ),
          FunctionName.of("ARRAYCONTAINS"),
          ArrayContainsKudf.class
      ));

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_BOOLEAN_SCHEMA,
          ImmutableList.of(
              SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(),
              Schema.OPTIONAL_INT64_SCHEMA
          ),
          FunctionName.of("ARRAYCONTAINS"),
          ArrayContainsKudf.class
      ));

      addBuiltInFunction(KaypherScalarFunction.createLegacyBuiltIn(
          Schema.OPTIONAL_BOOLEAN_SCHEMA,
          ImmutableList.of(
              SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build(),
              Schema.OPTIONAL_FLOAT64_SCHEMA
          ),
          FunctionName.of("ARRAYCONTAINS"),
          ArrayContainsKudf.class
      ));
    }

    private void addStructFieldFetcher() {

      addBuiltInFunction(
          KaypherScalarFunction.createLegacyBuiltIn(
              SchemaBuilder.struct().optional().build(),
              ImmutableList.of(
                  SchemaBuilder.struct().optional().build(),
                  Schema.STRING_SCHEMA
              ),
              FetchFieldFromStruct.FUNCTION_NAME,
              FetchFieldFromStruct.class
          ),
          true
      );
    }

    private void addUdafFunctions() {

      functionRegistry.addAggregateFunctionFactory(new CountAggFunctionFactory());
      functionRegistry.addAggregateFunctionFactory(new SumAggFunctionFactory());

      functionRegistry.addAggregateFunctionFactory(new MaxAggFunctionFactory());
      functionRegistry.addAggregateFunctionFactory(new MinAggFunctionFactory());

      functionRegistry.addAggregateFunctionFactory(new TopKAggregateFunctionFactory());
      functionRegistry.addAggregateFunctionFactory(new TopkDistinctAggFunctionFactory());
    }

    private void addBuiltInFunction(final KaypherScalarFunction kaypherFunction) {
      addBuiltInFunction(kaypherFunction, false);
    }

    private void addBuiltInFunction(final KaypherScalarFunction kaypherFunction, final boolean internal) {
      functionRegistry
          .ensureFunctionFactory(builtInUdfFactory(kaypherFunction, internal))
          .addFunction(kaypherFunction);
    }
  }
}
