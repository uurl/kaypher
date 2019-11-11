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
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import java.util.Objects;

/**
 * Base for all SQL types in KAYPHER.
 */
@Immutable
public abstract class SqlType {

  private final SqlBaseType baseType;

  SqlType(final SqlBaseType baseType) {
    this.baseType = Objects.requireNonNull(baseType, "baseType");
  }

  public SqlBaseType baseType() {
    return baseType;
  }

  public abstract boolean supportsCast();

  public abstract void validateValue(Object value);

  public abstract String toString(FormatOptions formatOptions);
}
