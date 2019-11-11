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
import com.treutec.kaypher.function.udf.Kudf;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.connect.data.Schema;

@Immutable
public final class KaypherFunction {

  static final String INTERNAL_PATH = "internal";

  private final Schema returnType;
  private final List<Schema> arguments;
  private final String functionName;
  private final Class<? extends Kudf> kudfClass;
  private final Function<KaypherConfig, Kudf> udfFactory;
  private final String description;
  private final String pathLoadedFrom;

  /**
   * Create built in / legacy function.
   */
  @SuppressWarnings("deprecation")  // Solution only available in Java9.
  public static KaypherFunction createLegacyBuiltIn(
      final Schema returnType,
      final List<Schema> arguments,
      final String functionName,
      final Class<? extends Kudf> kudfClass
  ) {
    final Function<KaypherConfig, Kudf> udfFactory = kaypherConfig -> {
      try {
        return kudfClass.newInstance();
      } catch (final Exception e) {
        throw new KaypherException("Failed to create instance of kudfClass "
            + kudfClass + " for function " + functionName, e);
      }
    };

    return create(
        returnType, arguments, functionName, kudfClass, udfFactory, "", INTERNAL_PATH);
  }

  /**
   * Create udf.
   *
   * <p>Can be either built-in UDF or true user-supplied.
   */
  static KaypherFunction create(
      final Schema returnType,
      final List<Schema> arguments,
      final String functionName,
      final Class<? extends Kudf> kudfClass,
      final Function<KaypherConfig, Kudf> udfFactory,
      final String description,
      final String pathLoadedFrom
  ) {
    return new KaypherFunction(
        returnType, arguments, functionName, kudfClass, udfFactory, description, pathLoadedFrom);
  }

  private KaypherFunction(
      final Schema returnType,
      final List<Schema> arguments,
      final String functionName,
      final Class<? extends Kudf> kudfClass,
      final Function<KaypherConfig, Kudf> udfFactory,
      final String description,
      final String pathLoadedFrom
  ) {
    this.returnType = Objects.requireNonNull(returnType, "returnType");
    this.arguments = ImmutableList.copyOf(Objects.requireNonNull(arguments, "arguments"));
    this.functionName = Objects.requireNonNull(functionName, "functionName");
    this.kudfClass = Objects.requireNonNull(kudfClass, "kudfClass");
    this.udfFactory = Objects.requireNonNull(udfFactory, "udfFactory");
    this.description = Objects.requireNonNull(description, "description");
    this.pathLoadedFrom  = Objects.requireNonNull(pathLoadedFrom, "pathLoadedFrom");

    if (arguments.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("KAYPHER Function can't have null argument types");
    }
  }

  public Schema getReturnType() {
    return returnType;
  }

  public List<Schema> getArguments() {
    return arguments;
  }

  String getFunctionName() {
    return functionName;
  }

  public String getDescription() {
    return description;
  }

  public Class<? extends Kudf> getKudfClass() {
    return kudfClass;
  }

  public String getPathLoadedFrom() {
    return pathLoadedFrom;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KaypherFunction that = (KaypherFunction) o;
    return Objects.equals(returnType, that.returnType)
        && Objects.equals(arguments, that.arguments)
        && Objects.equals(functionName, that.functionName)
        && Objects.equals(kudfClass, that.kudfClass)
        && Objects.equals(pathLoadedFrom, that.pathLoadedFrom);
  }

  @Override
  public int hashCode() {
    return Objects.hash(returnType, arguments, functionName, kudfClass, pathLoadedFrom);
  }

  @Override
  public String toString() {
    return "KaypherFunction{"
        + "returnType=" + returnType
        + ", arguments=" + arguments.stream().map(Schema::type).collect(Collectors.toList())
        + ", functionName='" + functionName + '\''
        + ", kudfClass=" + kudfClass
        + ", description='" + description + "'"
        + ", pathLoadedFrom='" + pathLoadedFrom + "'"
        + '}';
  }

  public Kudf newInstance(final KaypherConfig kaypherConfig) {
    return udfFactory.apply(kaypherConfig);
  }
}