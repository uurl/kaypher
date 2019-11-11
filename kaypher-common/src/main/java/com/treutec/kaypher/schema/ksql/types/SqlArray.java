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

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.schema.kaypher.DataException;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.schema.kaypher.SchemaConverters;
import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

@Immutable
public final class SqlArray extends SqlType {

  private final SqlType itemType;

  public static SqlArray of(final SqlType itemType) {
    return new SqlArray(itemType);
  }

  private SqlArray(final SqlType itemType) {
    super(SqlBaseType.ARRAY);
    this.itemType = requireNonNull(itemType, "itemType");
  }

  public SqlType getItemType() {
    return itemType;
  }

  @Override
  public boolean supportsCast() {
    return false;
  }

  @Override
  public void validateValue(final Object value) {
    if (value == null) {
      return;
    }

    if (!(value instanceof List)) {
      final SqlBaseType sqlBaseType = SchemaConverters.javaToSqlConverter()
          .toSqlType(value.getClass());

      throw new DataException("Expected ARRAY, got " + sqlBaseType);
    }

    final List<?> array = (List<?>) value;

    IntStream.range(0, array.size()).forEach(idx -> {
      try {
        final Object element = array.get(idx);
        itemType.validateValue(element);
      } catch (final DataException e) {
        throw new DataException("ARRAY element " + (idx + 1) + ": " + e.getMessage(), e);
      }
    });
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlArray array = (SqlArray) o;
    return Objects.equals(itemType, array.itemType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(itemType);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
    return "ARRAY<" + itemType.toString(formatOptions) + '>';
  }
}
