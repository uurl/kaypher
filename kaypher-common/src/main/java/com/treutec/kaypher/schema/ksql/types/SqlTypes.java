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

import com.treutec.kaypher.schema.kaypher.SqlBaseType;

public final class SqlTypes {

  private SqlTypes() {
  }

  public static final SqlPrimitiveType BOOLEAN = SqlPrimitiveType.of(SqlBaseType.BOOLEAN);
  public static final SqlPrimitiveType INTEGER = SqlPrimitiveType.of(SqlBaseType.INTEGER);
  public static final SqlPrimitiveType BIGINT = SqlPrimitiveType.of(SqlBaseType.BIGINT);
  public static final SqlPrimitiveType DOUBLE = SqlPrimitiveType.of(SqlBaseType.DOUBLE);
  public static final SqlPrimitiveType STRING = SqlPrimitiveType.of(SqlBaseType.STRING);

  public static SqlDecimal decimal(final int precision, final int scale) {
    return SqlDecimal.of(precision, scale);
  }

  public static SqlArray array(final SqlType elementType) {
    return SqlArray.of(elementType);
  }

  public static SqlMap map(final SqlType valueType) {
    return SqlMap.of(valueType);
  }

  public static SqlStruct.Builder struct() {
    return SqlStruct.builder();
  }

  /**
   * Schema of an INT up-cast to a DECIMAL
   */
  public static final SqlDecimal INT_UPCAST_TO_DECIMAL = SqlDecimal.of(10, 0);

  /**
   * Schema of an BIGINT up-cast to a DECIMAL
   */
  public static final SqlDecimal BIGINT_UPCAST_TO_DECIMAL = SqlDecimal.of(19, 0);
}
