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

import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.schema.kaypher.DataException;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.schema.kaypher.SchemaConverters;
import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import com.treutec.kaypher.util.DecimalUtil;
import java.math.BigDecimal;
import java.util.Objects;

@Immutable
public final class SqlDecimal extends SqlType {

  private final int precision;
  private final int scale;

  public static SqlDecimal of(final int precision, final int scale) {
    return new SqlDecimal(precision, scale);
  }

  private SqlDecimal(final int precision, final int scale) {
    super(SqlBaseType.DECIMAL);
    this.precision = precision;
    this.scale = scale;

    DecimalUtil.validateParameters(precision, scale);
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
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

    if (!(value instanceof BigDecimal)) {
      final SqlBaseType sqlBaseType = SchemaConverters.javaToSqlConverter()
          .toSqlType(value.getClass());

      throw new DataException("Expected DECIMAL, got " + sqlBaseType);
    }

    final BigDecimal decimal = (BigDecimal) value;
    if (decimal.precision() != precision) {
      throw new DataException("Expected " + this + ", got precision " + decimal.precision());
    }

    if (decimal.scale() != scale) {
      throw new DataException("Expected " + this + ", got scale " + decimal.scale());
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SqlDecimal that = (SqlDecimal) o;
    return precision == that.precision
        && scale == that.scale;
  }

  @Override
  public int hashCode() {
    return Objects.hash(precision, scale);
  }

  @Override
  public String toString() {
    return "DECIMAL(" + precision + ", " + scale + ')';
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
    return toString();
  }

  /**
   * Determine the decimal type should two decimals be added together.
   *
   * @param left the left side decimal.
   * @param right the right side decimal.
   * @return the resulting decimal type.
   */
  public static SqlDecimal add(final SqlDecimal left, final SqlDecimal right) {
    final int precision = Math.max(left.scale, right.scale)
        + Math.max(left.precision - left.scale, right.precision - right.scale)
        + 1;

    final int scale = Math.max(left.scale, right.scale);
    return SqlDecimal.of(precision, scale);
  }

  /**
   * Determine the decimal type should one decimal be subtracted from another.
   *
   * @param left the left side decimal.
   * @param right the right side decimal.
   * @return the resulting decimal type.
   */
  public static SqlDecimal subtract(final SqlDecimal left, final SqlDecimal right) {
    return add(left, right);
  }

  /**
   * Determine the decimal type should one decimal be multiplied by another.
   *
   * @param left the left side decimal.
   * @param right the right side decimal.
   * @return the resulting decimal type.
   */
  public static SqlDecimal multiply(final SqlDecimal left, final SqlDecimal right) {
    final int precision = left.precision + right.precision + 1;
    final int scale = left.scale + right.scale;
    return SqlDecimal.of(precision, scale);
  }

  /**
   * Determine the decimal type should one decimal be divided by another.
   *
   * @param left the left side decimal.
   * @param right the right side decimal.
   * @return the resulting decimal type.
   */
  public static SqlDecimal divide(final SqlDecimal left, final SqlDecimal right) {
    final int precision = left.precision - left.scale + right.scale
        + Math.max(6, left.scale + right.precision + 1);

    final int scale = Math.max(6, left.scale + right.precision + 1);
    return SqlDecimal.of(precision, scale);
  }

  /**
   * Determine the decimal result type when calculating the remainder of dividing one decimal by
   * another.
   *
   * @param left the left side decimal.
   * @param right the right side decimal.
   * @return the resulting decimal type.
   */
  public static SqlDecimal modulus(final SqlDecimal left, final SqlDecimal right) {
    final int precision = Math.min(left.precision - left.scale, right.precision - right.scale)
        + Math.max(left.scale, right.scale);

    final int scale = Math.max(left.scale, right.scale);
    return SqlDecimal.of(precision, scale);
  }
}
