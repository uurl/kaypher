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

import com.treutec.kaypher.function.udf.PluggableUdf;
import com.treutec.kaypher.function.udf.UdfMetadata;
import com.treutec.kaypher.function.udtf.Udtf;
import com.treutec.kaypher.function.udtf.UdtfDescription;
import com.treutec.kaypher.name.FunctionName;
import com.treutec.kaypher.schema.kaypher.SqlTypeParser;
import com.treutec.kaypher.util.KaypherException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads user defined table functions (UDTFs)
 */
public class UdtfLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(UdtfLoader.class);

  private final MutableFunctionRegistry functionRegistry;
  private final Optional<Metrics> metrics;
  private final SqlTypeParser typeParser;
  private final boolean throwExceptionOnLoadFailure;

  public UdtfLoader(
      final MutableFunctionRegistry functionRegistry,
      final Optional<Metrics> metrics,
      final SqlTypeParser typeParser,
      final boolean throwExceptionOnLoadFailure
  ) {
    this.functionRegistry = functionRegistry;
    this.metrics = metrics;
    this.typeParser = typeParser;
    this.throwExceptionOnLoadFailure = throwExceptionOnLoadFailure;
  }

  public void loadUdtfFromClass(
      final Class<?> theClass,
      final String path
  ) {
    final UdtfDescription udtfDescriptionAnnotation = theClass.getAnnotation(UdtfDescription.class);
    if (udtfDescriptionAnnotation == null) {
      throw new KaypherException(String.format("Cannot load class %s. Classes containing UDTFs must"
          + "be annotated with @UdtfDescription.", theClass.getName()));
    }
    final String functionName = udtfDescriptionAnnotation.name();
    final String sensorName = "kaypher-udtf-" + functionName;
    FunctionLoaderUtils.addSensor(sensorName, functionName, metrics);

    final UdfMetadata metadata = new UdfMetadata(
        udtfDescriptionAnnotation.name(),
        udtfDescriptionAnnotation.description(),
        udtfDescriptionAnnotation.author(),
        udtfDescriptionAnnotation.version(),
        path,
        false
    );

    final TableFunctionFactory factory = new TableFunctionFactory(metadata);

    for (Method method : theClass.getMethods()) {
      if (method.getAnnotation(Udtf.class) != null) {
        final Udtf annotation = method.getAnnotation(Udtf.class);
        try {
          if (method.getReturnType() != List.class) {
            throw new KaypherException(String
                .format("UDTF functions must return a List. Class %s Method %s",
                    theClass.getName(), method.getName()
                ));
          }
          final Type ret = method.getGenericReturnType();
          if (!(ret instanceof ParameterizedType)) {
            throw new KaypherException(String
                .format(
                    "UDTF functions must return a parameterized List. Class %s Method %s",
                    theClass.getName(), method.getName()
                ));
          }
          final Type typeArg = ((ParameterizedType) ret).getActualTypeArguments()[0];
          final Schema returnType = FunctionLoaderUtils
              .getReturnType(method, typeArg, annotation.schema(), typeParser);
          final List<Schema> parameters = FunctionLoaderUtils
              .createParameters(method, functionName, typeParser);
          final KaypherTableFunction tableFunction =
              createTableFunction(method, FunctionName.of(functionName), returnType,
                  parameters,
                  udtfDescriptionAnnotation.description(),
                  annotation
              );
          factory.addFunction(tableFunction);
        } catch (final KaypherException e) {
          if (throwExceptionOnLoadFailure) {
            throw e;
          } else {
            LOGGER.warn(
                "Failed to add UDTF to the MetaStore. name={} method={}",
                udtfDescriptionAnnotation.name(),
                method,
                e
            );
          }
        }
      }
    }

    functionRegistry.addTableFunctionFactory(factory);
  }

  private KaypherTableFunction createTableFunction(
      final Method method,
      final FunctionName functionName,
      final Schema outputType,
      final List<Schema> arguments,
      final String description,
      final Udtf udtfAnnotation
  ) {
    final FunctionInvoker invoker = FunctionLoaderUtils.createFunctionInvoker(method);
    final Object instance = FunctionLoaderUtils
        .instantiateFunctionInstance(method.getDeclaringClass(), description);
    final Function<List<Schema>, Schema> schemaProviderFunction = FunctionLoaderUtils
        .handleUdfReturnSchema(
            method.getDeclaringClass(),
            outputType,
            udtfAnnotation.schemaProvider(),
            functionName.name()
        );
    return new KaypherTableFunction(
        schemaProviderFunction,
        functionName, outputType, arguments, description,
        new PluggableUdf(invoker, instance)
    );
  }
}
