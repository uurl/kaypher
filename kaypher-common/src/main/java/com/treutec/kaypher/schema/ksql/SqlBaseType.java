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

/**
 * The SQL types supported by KAYPHER.
 */
public enum SqlBaseType {
  BOOLEAN, INTEGER, BIGINT, DECIMAL, DOUBLE, STRING, ARRAY, MAP, STRUCT;

  /**
   * @return {@code true} if numeric type.
   */
  public boolean isNumber() {
    return this == INTEGER || this == BIGINT || this == DECIMAL || this == DOUBLE;
  }

  /**
   * Test to see if this type can be <i>implicitly</i> cast to another.
   *
   * <p>Types can always be cast to themselves. Only numeric types can be implicitly cast to other
   * numeric types. Note: STRING to DECIMAL handling is not seen as casting: it's parsing.
   *
   * @param to the target type.
   * @return true if this type can be implicitly cast to the supplied type.
   */
  public boolean canImplicitlyCast(final SqlBaseType to) {
    return this.equals(to)
        || (isNumber() && to.isNumber() && this.ordinal() <= to.ordinal());
  }
}
