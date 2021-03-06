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
package com.treutec.kaypher.serde;

import com.google.common.collect.ImmutableSet;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.properties.with.CommonCreateConfigs;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Factory / Util class for building the {@link SerdeOption} sets required by the engine.
 */
public final class SerdeOptions {

  private SerdeOptions() {
  }

  /**
   * Build the default options to be used by statements, should not be explicitly provided.
   *
   * @param kaypherConfig the system config containing defaults.
   * @return the set of default serde options.
   */
  public static Set<SerdeOption> buildDefaults(final KaypherConfig kaypherConfig) {
    final ImmutableSet.Builder<SerdeOption> options = ImmutableSet.builder();

    if (!kaypherConfig.getBoolean(KaypherConfig.KAYPHER_WRAP_SINGLE_VALUES)) {
      options.add(SerdeOption.UNWRAP_SINGLE_VALUES);
    }

    return ImmutableSet.copyOf(options.build());
  }

  /**
   * Build serde options for {@code `CREATE STREAM`} and {@code `CREATE TABLE`} statements.
   *
   * @param schema the logical schema of the create statement.
   * @param valueFormat the format of the value.
   * @param wrapSingleValues explicitly set single value wrapping flag.
   * @param kaypherConfig the system config, used to retrieve defaults.
   * @return the set of serde options the statement defines.
   */
  public static Set<SerdeOption> buildForCreateStatement(
      final LogicalSchema schema,
      final Format valueFormat,
      final Optional<Boolean> wrapSingleValues,
      final KaypherConfig kaypherConfig
  ) {
    final boolean singleField = schema.valueConnectSchema().fields().size() == 1;
    final Set<SerdeOption> singleFieldDefaults = buildDefaults(kaypherConfig);
    return build(singleField, valueFormat, wrapSingleValues, singleFieldDefaults);
  }

  /**
   * Build serde options for {@code `CREATE STREAM AS SELECT`} and {@code `CREATE TABLE AS SELECT`}
   * statements.
   *
   * @param valueColumnNames the set of column names in the schema.
   * @param valueFormat the format of the value.
   * @param wrapSingleValues explicitly set single value wrapping flag.
   * @param singleFieldDefaults the defaults for single fields.
   * @return the set of serde options the statement defines.
   */
  public static Set<SerdeOption> buildForCreateAsStatement(
      final List<ColumnName> valueColumnNames,
      final Format valueFormat,
      final Optional<Boolean> wrapSingleValues,
      final Set<SerdeOption> singleFieldDefaults
  ) {
    final boolean singleField = valueColumnNames.size() == 1;
    return build(singleField, valueFormat, wrapSingleValues, singleFieldDefaults);
  }

  private static Set<SerdeOption> build(
      final boolean singleField,
      final Format valueFormat,
      final Optional<Boolean> wrapSingleValues,
      final Set<SerdeOption> singleFieldDefaults
  ) {
    if (!wrapSingleValues.isPresent()) {
      return singleField && singleFieldDefaults.contains(SerdeOption.UNWRAP_SINGLE_VALUES)
          ? SerdeOption.of(SerdeOption.UNWRAP_SINGLE_VALUES)
          : SerdeOption.none();
    }

    if (!valueFormat.supportsUnwrapping()) {
      throw new KaypherException("'" + CommonCreateConfigs.WRAP_SINGLE_VALUE
          + "' can not be used with format '"
          + valueFormat + "' as it does not support wrapping");
    }

    if (!singleField) {
      throw new KaypherException("'" + CommonCreateConfigs.WRAP_SINGLE_VALUE
          + "' is only valid for single-field value schemas");
    }

    final ImmutableSet.Builder<SerdeOption> options = ImmutableSet.builder();

    if (!wrapSingleValues.get()) {
      options.add(SerdeOption.UNWRAP_SINGLE_VALUES);
    }

    return options.build();
  }
}
