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

package com.treutec.kaypher.schema;

import static java.util.Objects.requireNonNull;

import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import com.treutec.kaypher.schema.kaypher.types.SqlDecimal;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.util.KaypherException;
import java.util.function.BinaryOperator;

public enum Operator {
  ADD("+", SqlDecimal::add) {
    @Override
    public SqlType resultType(final SqlType left, final SqlType right) {
      if (left.baseType() == SqlBaseType.STRING && right.baseType() == SqlBaseType.STRING) {
        return SqlTypes.STRING;
      }

      return super.resultType(left, right);
    }
  },
  SUBTRACT("-", SqlDecimal::subtract),
  MULTIPLY("*", SqlDecimal::multiply),
  DIVIDE("/", SqlDecimal::divide),
  MODULUS("%", SqlDecimal::modulus);

  private final String symbol;
  private final BinaryOperator<SqlDecimal> binaryResolver;

  Operator(final String symbol, final BinaryOperator<SqlDecimal> binaryResolver) {
    this.symbol = requireNonNull(symbol, "symbol");
    this.binaryResolver = requireNonNull(binaryResolver, "binaryResolver");
  }

  public String getSymbol() {
    return symbol;
  }

  /**
   * Determine the result type for the given parameters.
   *
   * @param left the left side of the operation.
   * @param right the right side of the operation.
   * @return the result schema.
   */
  public SqlType resultType(final SqlType left, final SqlType right) {
    if (left.baseType().isNumber() && right.baseType().isNumber()) {
      if (left.baseType().canImplicitlyCast(right.baseType())) {
        if (right.baseType() != SqlBaseType.DECIMAL) {
          return right;
        }

        return binaryResolver.apply(toDecimal(left), (SqlDecimal) right);
      }

      if (right.baseType().canImplicitlyCast(left.baseType())) {
        if (left.baseType() != SqlBaseType.DECIMAL) {
          return left;
        }

        return binaryResolver.apply((SqlDecimal) left, toDecimal(right));
      }
    }

    throw new KaypherException(
        "Unsupported arithmetic types. " + left.baseType() + " " + right.baseType());
  }

  private static SqlDecimal toDecimal(final SqlType type) {
    switch (type.baseType()) {
      case DECIMAL:
        return (SqlDecimal) type;
      case INTEGER:
        return SqlTypes.INT_UPCAST_TO_DECIMAL;
      case BIGINT:
        return SqlTypes.BIGINT_UPCAST_TO_DECIMAL;
      default:
        throw new KaypherException(
            "Cannot convert " + type.baseType() + " to " + SqlBaseType.DECIMAL + ".");
    }
  }
}
