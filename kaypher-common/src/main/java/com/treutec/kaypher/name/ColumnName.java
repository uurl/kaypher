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

package com.treutec.kaypher.name;

import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.schema.kaypher.ColumnRef;

/**
 * The name of a column within a source.
 */
@Immutable
public final class ColumnName extends Name<ColumnName> {

  private static final String AGGREGATE_COLUMN_PREFIX = "KAYPHER_AGG_VARIABLE_";
  private static final String GENERATED_ALIAS_PREFIX = "KAYPHER_COL_";
  private static final String SYNTHESISED_COLUMN_PREFIX = "KAYPHER_SYNTH_";

  public static ColumnName aggregateColumn(final int idx) {
    return of(AGGREGATE_COLUMN_PREFIX + idx);
  }

  /**
   * Where the user hasn't specified an alias for an expression in a SELECT we generate them
   * using this method. This value is exposed to the user in the output schema
   */
  public static ColumnName generatedColumnAlias(final int idx) {
    return ColumnName.of(GENERATED_ALIAS_PREFIX + idx);
  }

  /**
   * Used to generate a column name in an intermediate schema, e.g. for a column to hold
   * values of a table function. These are never exposed to the user
   */
  public static ColumnName synthesisedSchemaColumn(final int idx) {
    return ColumnName.of(SYNTHESISED_COLUMN_PREFIX + idx);
  }

  /**
   * Used to generate a column alias for a join where the a column with this name exists
   * in both of the sources.
   */
  public static ColumnName generatedJoinColumnAlias(final ColumnRef ref) {
    return ref.source()
        .map(q -> q.name() + "_" + ref.name().name())
        .map(ColumnName::of)
        .orElseGet(ref::name);
  }

  public static ColumnName of(final String name) {
    return new ColumnName(name);
  }

  private ColumnName(final String name) {
    super(name);
  }

  public boolean isAggregate() {
    return name.startsWith(AGGREGATE_COLUMN_PREFIX);
  }

}
