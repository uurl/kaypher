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

import com.treutec.kaypher.function.udf.Kudf;
import com.treutec.kaypher.function.udf.UdfMetadata;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.SchemaUtil;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.connect.data.Schema;

public class UdfFactory {
  private final UdfMetadata metadata;
  private final Class<? extends Kudf> udfClass;
  private final Map<List<FunctionParameter>, KaypherFunction> functions = new LinkedHashMap<>();


  UdfFactory(final Class<? extends Kudf> udfClass,
             final UdfMetadata metadata) {
    this.udfClass = Objects.requireNonNull(udfClass, "udfClass can't be null");
    this.metadata = Objects.requireNonNull(metadata, "metadata can't be null");
  }

  public UdfFactory copy() {
    final UdfFactory udf = new UdfFactory(udfClass, metadata);
    udf.functions.putAll(functions);
    return udf;
  }

  void addFunction(final KaypherFunction kaypherFunction) {
    final List<FunctionParameter> paramTypes
        = mapToFunctionParameter(kaypherFunction.getArguments());

    checkCompatible(kaypherFunction, paramTypes);
    functions.put(paramTypes, kaypherFunction);
  }

  private void checkCompatible(final KaypherFunction kaypherFunction,
                               final List<FunctionParameter> paramTypes) {
    if (udfClass != kaypherFunction.getKudfClass()) {
      throw new KaypherException("Can't add function " + kaypherFunction
          + " as a function with the same name exists in a different " + udfClass);
    }
    if (functions.containsKey(paramTypes)) {
      throw new KaypherException("Can't add function " + kaypherFunction
          + " as a function with the same name and argument types already exists "
          + functions.get(paramTypes));
    }
    if (!kaypherFunction.getPathLoadedFrom().equals(metadata.getPath())) {
      throw new KaypherException("Can't add function " + kaypherFunction
          + "as a function with the same name has been loaded from a different jar "
          + metadata.getPath());
    }
  }

  public String getName() {
    return metadata.getName();
  }

  public String getAuthor() {
    return metadata.getAuthor();
  }

  public String getVersion() {
    return metadata.getVersion();
  }

  public String getDescription() {
    return metadata.getDescription();
  }

  public void eachFunction(final Consumer<KaypherFunction> consumer) {
    functions.values().forEach(consumer);
  }

  public boolean isInternal() {
    return metadata.isInternal();
  }

  public String getPath() {
    return metadata.getPath();
  }

  public boolean matches(final UdfFactory that) {
    return this == that
        || (this.udfClass.equals(that.udfClass) && this.metadata.equals(that.metadata));
  }

  @Override
  public String toString() {
    return "UdfFactory{"
        + "metadata=" + metadata
        + ", udfClass=" + udfClass
        + ", functions=" + functions
        + '}';
  }

  public KaypherFunction getFunction(final List<Schema> paramTypes) {
    final List<FunctionParameter> params = mapToFunctionParameter(paramTypes);
    final KaypherFunction function = functions.get(params);
    if (function != null) {
      return function;
    }

    if (paramTypes.stream().anyMatch(Objects::isNull)) {
      return functions.entrySet()
          .stream()
          .filter(entry -> checkParamsMatch(entry.getKey(), paramTypes))
          .map(Map.Entry::getValue)
          .findFirst()
          .orElseThrow(() -> createNoMatchingFunctionException(paramTypes));
    }

    throw createNoMatchingFunctionException(paramTypes);
  }

  private KaypherException createNoMatchingFunctionException(final List<Schema> paramTypes) {
    final String sqlParamTypes = paramTypes.stream()
        .map(schema -> schema == null
            ? null
            : SchemaUtil.getSchemaTypeAsSqlType(schema.type()))
        .collect(Collectors.joining(", ", "[", "]"));

    return new KaypherException("Function '" + metadata.getName()
                            + "' does not accept parameters of types:" + sqlParamTypes);
  }

  private static boolean checkParamsMatch(final List<FunctionParameter> functionArgTypes,
                                          final List<Schema> suppliedParamTypes) {
    if (functionArgTypes.size() != suppliedParamTypes.size()) {
      return false;
    }

    return IntStream.range(0, suppliedParamTypes.size())
        .boxed()
        .allMatch(idx -> functionArgTypes.get(idx).matches(suppliedParamTypes.get(idx)));
  }

  private List<FunctionParameter> mapToFunctionParameter(final List<Schema> params) {
    return params
        .stream()
        .map(schema -> schema == null
            ? new FunctionParameter(null, false)
            : new FunctionParameter(schema.type(), schema.isOptional()))
        .collect(Collectors.toList());
  }

  private static final class FunctionParameter {
    private final Schema.Type type;
    private final boolean isOptional;

    private FunctionParameter(final Schema.Type type, final boolean isOptional) {
      this.type = type;
      this.isOptional = isOptional;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final FunctionParameter that = (FunctionParameter) o;
      // isOptional is excluded from equals and hashCode so that
      // primitive types will match their boxed counterparts. i.e,
      // primitive types are not optional, i.e., they don't accept null.
      return type == that.type;
    }

    @Override
    public int hashCode() {
      // isOptional is excluded from equals and hashCode so that
      // primitive types will match their boxed counterparts. i.e,
      // primitive types are not optional, i.e., they don't accept null.
      return Objects.hash(type);
    }

    boolean isOptional() {
      return isOptional;
    }

    public boolean matches(final Schema schema) {
      if (schema == null) {
        return isOptional;
      }
      return type.equals(schema.type());
    }
  }
}
