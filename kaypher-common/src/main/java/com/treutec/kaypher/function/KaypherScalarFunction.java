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
import com.treutec.kaypher.name.FunctionName;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.connect.data.Schema;

@Immutable
public final class KaypherScalarFunction extends KaypherFunction {

  static final String INTERNAL_PATH = "internal";

  private final Class<? extends Kudf> kudfClass;
  private final Function<KaypherConfig, Kudf> udfFactory;

  private KaypherScalarFunction(
      final Function<List<Schema>, Schema> returnSchemaProvider,
      final Schema javaReturnType,
      final List<Schema> arguments,
      final FunctionName functionName,
      final Class<? extends Kudf> kudfClass,
      final Function<KaypherConfig, Kudf> udfFactory,
      final String description,
      final String pathLoadedFrom,
      final boolean isVariadic
  ) {
    super(returnSchemaProvider, javaReturnType, arguments, functionName, description,
        pathLoadedFrom, isVariadic
    );
    this.kudfClass = Objects.requireNonNull(kudfClass, "kudfClass");
    this.udfFactory = Objects.requireNonNull(udfFactory, "udfFactory");
  }

  /**
   * Create built in / legacy function.
   */
  @SuppressWarnings("deprecation")  // Solution only available in Java9.
  public static KaypherScalarFunction createLegacyBuiltIn(
      final Schema returnType,
      final List<Schema> arguments,
      final FunctionName functionName,
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
        ignored -> returnType, returnType, arguments, functionName, kudfClass, udfFactory, "",
        INTERNAL_PATH, false
    );
  }

  public Class<? extends Kudf> getKudfClass() {
    return kudfClass;
  }

  /**
   * Create udf.
   *
   * <p>Can be either built-in UDF or true user-supplied.
   */
  static KaypherScalarFunction create(
      final Function<List<Schema>, Schema> schemaProvider,
      final Schema javaReturnType,
      final List<Schema> arguments,
      final FunctionName functionName,
      final Class<? extends Kudf> kudfClass,
      final Function<KaypherConfig, Kudf> udfFactory,
      final String description,
      final String pathLoadedFrom,
      final boolean isVariadic
  ) {
    return new KaypherScalarFunction(
        schemaProvider,
        javaReturnType,
        arguments,
        functionName,
        kudfClass,
        udfFactory,
        description,
        pathLoadedFrom,
        isVariadic
    );
  }

  @Override
  public String toString() {
    return "KaypherFunction{"
        + ", kudfClass=" + kudfClass
        + '}';
  }

  public Kudf newInstance(final KaypherConfig kaypherConfig) {
    return udfFactory.apply(kaypherConfig);
  }
}