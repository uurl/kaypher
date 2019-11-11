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

import com.google.common.annotations.VisibleForTesting;
import com.treutec.kaypher.execution.function.UdfUtil;
import com.treutec.kaypher.function.udf.UdfParameter;
import com.treutec.kaypher.function.udf.UdfSchemaProvider;
import com.treutec.kaypher.schema.kaypher.SchemaConverters;
import com.treutec.kaypher.schema.kaypher.SqlTypeParser;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.util.DecimalUtil;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.SchemaUtil;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.connect.data.Schema;

/**
 * Utility class for loading different types of user defined funcrions
 */
public final class FunctionLoaderUtils {

  private static final String UDF_METRIC_GROUP = "kaypher-udf";

  private FunctionLoaderUtils() {
  }

  static List<Schema> createParameters(
      final Method method, final String functionName,
      final SqlTypeParser typeParser
  ) {
    return IntStream.range(0, method.getParameterCount()).mapToObj(idx -> {
      final Type type = method.getGenericParameterTypes()[idx];
      final Optional<UdfParameter> annotation = Arrays.stream(method.getParameterAnnotations()[idx])
          .filter(UdfParameter.class::isInstance)
          .map(UdfParameter.class::cast)
          .findAny();

      final Parameter param = method.getParameters()[idx];
      final String name = annotation.map(UdfParameter::value)
          .filter(val -> !val.isEmpty())
          .orElse(param.isNamePresent() ? param.getName() : "");

      if (name.trim().isEmpty()) {
        throw new KaypherFunctionException(
            String.format("Cannot resolve parameter name for param at index %d for UDF %s:%s. "
                    + "Please specify a name in @UdfParameter or compile your JAR with -parameters "
                    + "to infer the name from the parameter name.",
                idx, functionName, method.getName()
            ));
      }

      final String doc = annotation.map(UdfParameter::description).orElse("");
      if (annotation.isPresent() && !annotation.get().schema().isEmpty()) {
        return SchemaConverters.sqlToConnectConverter()
            .toConnectSchema(
                typeParser.parse(annotation.get().schema()).getSqlType(),
                name,
                doc
            );
      }

      return UdfUtil.getSchemaFromType(type, name, doc);
    }).collect(Collectors.toList());
  }

  @VisibleForTesting
  public static FunctionInvoker createFunctionInvoker(final Method method) {
    return new DynamicFunctionInvoker(method);
  }

  static Object instantiateFunctionInstance(
      final Class functionClass,
      final String functionName
  ) {
    try {
      return functionClass.newInstance();
    } catch (final Exception e) {
      throw new KaypherException(
          "Failed to create instance for UDF/UDTF="
              + functionName,
          e
      );
    }
  }

  static void addSensor(
      final String sensorName, final String udfName, final Optional<Metrics> theMetrics
  ) {
    theMetrics.ifPresent(metrics -> {
      if (metrics.getSensor(sensorName) == null) {
        final Sensor sensor = metrics.sensor(sensorName);
        sensor.add(
            metrics.metricName(sensorName + "-avg", UDF_METRIC_GROUP,
                "Average time for an invocation of " + udfName + " udf"
            ),
            new Avg()
        );
        sensor.add(
            metrics.metricName(sensorName + "-max", UDF_METRIC_GROUP,
                "Max time for an invocation of " + udfName + " udf"
            ),
            new Max()
        );
        sensor.add(
            metrics.metricName(sensorName + "-count", UDF_METRIC_GROUP,
                "Total number of invocations of " + udfName + " udf"
            ),
            new WindowedCount()
        );
        sensor.add(
            metrics.metricName(sensorName + "-rate", UDF_METRIC_GROUP,
                "The average number of occurrence of " + udfName + " operation per second "
                    + udfName + " udf"
            ),
            new Rate(TimeUnit.SECONDS, new WindowedCount())
        );
      }
    });
  }

  static Schema getReturnType(
      final Method method, final String annotationSchema,
      final SqlTypeParser typeParser
  ) {
    return getReturnType(method, method.getGenericReturnType(), annotationSchema, typeParser);
  }

  static Schema getReturnType(
      final Method method, final Type type, final String annotationSchema,
      final SqlTypeParser typeParser
  ) {
    try {
      final Schema returnType = annotationSchema.isEmpty()
          ? UdfUtil.getSchemaFromType(type)
          : SchemaConverters
              .sqlToConnectConverter()
              .toConnectSchema(
                  typeParser.parse(annotationSchema).getSqlType());

      return SchemaUtil.ensureOptional(returnType);
    } catch (final KaypherException e) {
      throw new KaypherException("Could not load UDF method with signature: " + method, e);
    }
  }

  static Function<List<Schema>, Schema> handleUdfReturnSchema(
      final Class theClass,
      final Schema javaReturnSchema,
      final String schemaProviderFunctionName,
      final String functionName
  ) {
    if (!schemaProviderFunctionName.equals("")) {
      return handleUdfSchemaProviderAnnotation(
          schemaProviderFunctionName, theClass, functionName);
    } else if (DecimalUtil.isDecimal(javaReturnSchema)) {
      throw new KaypherException(String.format("Cannot load UDF %s. BigDecimal return type "
          + "is not supported without a schema provider method.", functionName));
    }

    return ignored -> javaReturnSchema;
  }

  private static Function<List<Schema>, Schema> handleUdfSchemaProviderAnnotation(
      final String schemaProviderName,
      final Class theClass,
      final String functionName
  ) {
    // throws exception if cannot find method
    final Method m = findSchemaProvider(theClass, schemaProviderName);
    final Object instance = FunctionLoaderUtils
        .instantiateFunctionInstance(theClass, functionName);

    return parameterSchemas -> {
      final List<SqlType> parameterTypes = parameterSchemas.stream()
          .map(p -> SchemaConverters.connectToSqlConverter().toSqlType(p))
          .collect(Collectors.toList());
      return SchemaConverters.sqlToConnectConverter().toConnectSchema(invokeSchemaProviderMethod(
          instance, m, parameterTypes, functionName));
    };
  }

  private static Method findSchemaProvider(
      final Class<?> theClass,
      final String schemaProviderName
  ) {
    try {
      final Method m = theClass.getDeclaredMethod(schemaProviderName, List.class);
      if (!m.isAnnotationPresent(UdfSchemaProvider.class)) {
        throw new KaypherException(String.format(
            "Method %s should be annotated with @UdfSchemaProvider.",
            schemaProviderName
        ));
      }
      return m;
    } catch (NoSuchMethodException e) {
      throw new KaypherException(String.format(
          "Cannot find schema provider method with name %s and parameter List<SqlType> in class "
              + "%s.", schemaProviderName, theClass.getName()), e);
    }
  }

  private static SqlType invokeSchemaProviderMethod(
      final Object instance,
      final Method m,
      final List<SqlType> args,
      final String functionName
  ) {
    try {
      return (SqlType) m.invoke(instance, args);
    } catch (IllegalAccessException
        | InvocationTargetException e) {
      throw new KaypherException(String.format("Cannot invoke the schema provider "
              + "method %s for UDF %s. ",
          m.getName(), functionName
      ), e);
    }
  }

}
