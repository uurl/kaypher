/*
 * Copyright 2019 Koneksys
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

package com.koneksys.kaypher.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import java.util.Optional;

@Immutable
public abstract class Type extends Expression {

  public enum SqlType {
    BOOLEAN, INTEGER, BIGINT, DOUBLE, STRING, ARRAY, MAP, STRUCT
  }

  private final SqlType sqlType;

  protected Type(final Optional<NodeLocation> location, final SqlType sqlType) {
    super(location);
    this.sqlType = requireNonNull(sqlType, "sqlType");
  }

  public SqlType getSqlType() {
    return sqlType;
  }
}
