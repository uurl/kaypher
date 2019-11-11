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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.schema.kaypher.DataException;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.schema.kaypher.SchemaConverters;
import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import com.treutec.kaypher.types.KaypherStruct;
import com.treutec.kaypher.util.KaypherException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Immutable
public final class SqlStruct extends SqlType {

  private static final String PREFIX = "STRUCT<";
  private static final String POSTFIX = ">";
  private static final String EMPTY_STRUCT = PREFIX + " " + POSTFIX;

  private final ImmutableList<Field> fields;
  private final ImmutableMap<String, Field> byName;

  public static Builder builder() {
    return new Builder();
  }

  private SqlStruct(final List<Field> fields, final Map<String, Field> byName) {
    super(SqlBaseType.STRUCT);
    this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields"));
    this.byName = ImmutableMap.copyOf(requireNonNull(byName, "byName"));
  }

  public List<Field> fields() {
    return fields;
  }

  public Optional<Field> field(final String name) {
    return Optional.ofNullable(byName.get(name));
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

    if (!(value instanceof KaypherStruct)) {
      final SqlBaseType sqlBaseType = SchemaConverters.javaToSqlConverter()
          .toSqlType(value.getClass());

      throw new DataException("Expected STRUCT, got " + sqlBaseType);
    }

    final KaypherStruct struct = (KaypherStruct)value;
    if (!struct.schema().equals(this)) {
      throw new DataException("Expected " + this + ", got " + struct.schema());
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
    final SqlStruct struct = (SqlStruct) o;
    return fields.equals(struct.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  @Override
  public String toString(final FormatOptions formatOptions) {
    if (fields.isEmpty()) {
      return EMPTY_STRUCT;
    }

    return fields.stream()
        .map(f -> f.toString(formatOptions))
        .collect(Collectors.joining(", ", PREFIX, POSTFIX));
  }

  public static final class Builder {

    private final List<Field> fields = new ArrayList<>();
    private final Map<String, Field> byName = new HashMap<>();

    public Builder field(final String fieldName, final SqlType fieldType) {
      return field(Field.of(fieldName, fieldType));
    }

    public Builder field(final Field field) {
      if (byName.putIfAbsent(field.name(), field) != null) {
        throw new KaypherException("Duplicate field names found in STRUCT: "
            + "'" + byName.get(field.name()) + "' and '" + field + "'");
      }

      fields.add(field);
      return this;
    }

    public Builder fields(final Iterable<? extends Field> fields) {
      fields.forEach(this::field);
      return this;
    }

    public SqlStruct build() {
      return new SqlStruct(fields, byName);
    }
  }
}
