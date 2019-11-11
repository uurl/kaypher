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
package com.treutec.kaypher.schema.kaypher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.schema.kaypher.types.SqlArray;
import com.treutec.kaypher.schema.kaypher.types.SqlDecimal;
import com.treutec.kaypher.schema.kaypher.types.SqlMap;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.util.KaypherException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public final class DefaultSqlValueCoercer implements SqlValueCoercer {

  private static final Map<SqlBaseType, Function<Number, Number>> UPCASTER =
      ImmutableMap.<SqlBaseType, Function<Number, Number>>builder()
          .put(SqlBaseType.INTEGER, Number::intValue)
          .put(SqlBaseType.BIGINT, Number::longValue)
          .put(SqlBaseType.DOUBLE, Number::doubleValue)
          .build();

  @Override
  public Optional<?> coerce(final Object value, final SqlType targetType) {
    return doCoerce(value, targetType);
  }

  private static Optional<?> doCoerce(final Object value, final SqlType targetType) {
    switch (targetType.baseType()) {
      case DECIMAL:
        return coerceDecimal(value, (SqlDecimal) targetType);
      case ARRAY:
        return coerceArray(value, (SqlArray) targetType);
      case MAP:
        return coerceMap(value, (SqlMap) targetType);
      case STRUCT:
        throw new KaypherException("Unsupported SQL type: " + targetType.baseType());
      default:
        break;
    }

    final SqlBaseType valueSqlType = SchemaConverters.javaToSqlConverter()
        .toSqlType(value.getClass());

    if (valueSqlType.equals(targetType.baseType())) {
      return Optional.of(value);
    }

    if (!(value instanceof Number) || !valueSqlType.canImplicitlyCast(targetType.baseType())) {
      return Optional.empty();
    }

    final Number result = UPCASTER.get(targetType.baseType()).apply((Number) value);
    return Optional.of(result);
  }

  private static Optional<?> coerceArray(final Object value, final SqlArray targetType) {
    if (!(value instanceof List<?>)) {
      return Optional.empty();
    }

    final List<?> list = (List<?>) value;
    final ImmutableList.Builder<Object> coerced = ImmutableList.builder();
    for (final Object el : list) {
      final Optional<?> coercedEl = doCoerce(el, targetType.getItemType());
      if (!coercedEl.isPresent()) {
        return Optional.empty();
      }
      coerced.add(coercedEl.get());
    }

    return Optional.of(coerced.build());
  }

  private static Optional<?> coerceMap(final Object value, final SqlMap targetType) {
    if (!(value instanceof Map<?, ?>)) {
      return Optional.empty();
    }

    final Map<?, ?> map = (Map<?, ?>) value;
    final HashMap<Object, Object> coerced = new HashMap<>();
    for (final Map.Entry entry : map.entrySet()) {
      final Optional<?> coercedKey = doCoerce(entry.getKey(), SqlTypes.STRING);
      final Optional<?> coercedValue = doCoerce(entry.getValue(), targetType.getValueType());
      if (!coercedKey.isPresent() || !coercedValue.isPresent()) {
        return Optional.empty();
      }
      coerced.put(coercedKey.get(), coercedValue.get());
    }

    return Optional.of(coerced);
  }

  private static Optional<?> coerceDecimal(final Object value, final SqlDecimal targetType) {
    final int precision = targetType.getPrecision();
    final int scale = targetType.getScale();

    if (value instanceof String) {
      try {
        return Optional.of(new BigDecimal((String) value, new MathContext(precision))
            .setScale(scale, RoundingMode.UNNECESSARY));
      } catch (final NumberFormatException e) {
        throw new KaypherException("Cannot coerce value to DECIMAL: " + value, e);
      }
    }

    if (value instanceof Number && !(value instanceof Double)) {
      return Optional.of(
          new BigDecimal(
              ((Number) value).doubleValue(),
              new MathContext(precision))
              .setScale(scale, RoundingMode.UNNECESSARY));
    }

    return Optional.empty();
  }
}
