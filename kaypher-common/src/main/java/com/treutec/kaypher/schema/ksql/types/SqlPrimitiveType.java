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


package com.treutec.kaypher.schema.kaypher.types;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.schema.kaypher.DataException;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.schema.kaypher.SchemaConverters;
import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import com.treutec.kaypher.util.KaypherException;
import java.util.Objects;
import java.util.stream.Collectors;

@Immutable
public final class SqlPrimitiveType extends SqlType {

  private static final String INT = "INT";
  private static final String VARCHAR = "VARCHAR";

  private static final ImmutableMap<SqlBaseType, SqlPrimitiveType> TYPES =
      ImmutableMap.<SqlBaseType, SqlPrimitiveType>builder()
          .put(SqlBaseType.BOOLEAN, new SqlPrimitiveType(SqlBaseType.BOOLEAN))
          .put(SqlBaseType.INTEGER, new SqlPrimitiveType(SqlBaseType.INTEGER))
          .put(SqlBaseType.BIGINT, new SqlPrimitiveType(SqlBaseType.BIGINT))
          .put(SqlBaseType.DOUBLE, new SqlPrimitiveType(SqlBaseType.DOUBLE))
          .put(SqlBaseType.STRING, new SqlPrimitiveType(SqlBaseType.STRING))
          .build();

  private static final ImmutableSet<String> PRIMITIVE_TYPE_NAMES = ImmutableSet.<String>builder()
      .addAll(TYPES.keySet().stream().map(SqlBaseType::name).collect(Collectors.toList()))
      .add(INT)
      .add(VARCHAR)
      .build();

  public static boolean isPrimitiveTypeName(final String name) {
    return PRIMITIVE_TYPE_NAMES.contains(name.toUpperCase());
  }

  public static SqlPrimitiveType of(final String typeName) {
    switch (typeName.toUpperCase()) {
      case INT:
        return SqlPrimitiveType.of(SqlBaseType.INTEGER);
      case VARCHAR:
        return SqlPrimitiveType.of(SqlBaseType.STRING);
      default:
        try {
          final SqlBaseType sqlType = SqlBaseType.valueOf(typeName.toUpperCase());
          return SqlPrimitiveType.of(sqlType);
        } catch (final IllegalArgumentException e) {
          throw new KaypherException("Unknown primitive type: " + typeName, e);
        }
    }
  }

  public static SqlPrimitiveType of(final SqlBaseType sqlType) {
    final SqlPrimitiveType primitive = TYPES.get(Objects.requireNonNull(sqlType, "sqlType"));
    if (primitive == null) {
      throw new KaypherException("Invalid primitive type: " + sqlType);
    }
    return primitive;
  }

  private SqlPrimitiveType(final SqlBaseType baseType) {
    super(baseType);
  }

  @Override
  public boolean supportsCast() {
    return true;
  }

  @Override
  public void validateValue(final Object value) {
    if (value == null) {
      return;
    }

    final SqlBaseType actualType = SchemaConverters.javaToSqlConverter()
        .toSqlType(value.getClass());

    if (!baseType().equals(actualType)) {
      throw new DataException("Expected " + baseType() + ", got " + actualType);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SqlPrimitiveType)) {
      return false;
    }

    final SqlPrimitiveType that = (SqlPrimitiveType) o;
    return Objects.equals(this.baseType(), that.baseType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseType());
  }

  @Override
  public String toString() {
    return baseType().toString();
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
    return toString();
  }
}
