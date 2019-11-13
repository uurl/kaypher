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


package com.treutec.kaypher.types;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.schema.kaypher.types.Field;
import com.treutec.kaypher.schema.kaypher.types.SqlStruct;
import com.treutec.kaypher.util.KaypherException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * Instance of {@link com.treutec.kaypher.schema.kaypher.types.SqlStruct}.
 */
@Immutable
public final class KaypherStruct {

  private final SqlStruct schema;
  private final ImmutableList<Optional<?>> values;

  public static Builder builder(final SqlStruct schema) {
    return new Builder(schema);
  }

  private KaypherStruct(
      final SqlStruct schema,
      final List<Optional<?>> values
  ) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.values = ImmutableList.copyOf(Objects.requireNonNull(values, "values"));
  }

  public SqlStruct schema() {
    return this.schema;
  }

  public List<Optional<?>> values() {
    return values;
  }

  public void forEach(final BiConsumer<? super Field, ? super Optional<?>> consumer) {
    for (int idx = 0; idx < values.size(); idx++) {
      final Field field = schema.fields().get(idx);
      final Optional<?> value = values.get(idx);
      consumer.accept(field, value);
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
    final KaypherStruct that = (KaypherStruct) o;
    return Objects.equals(schema, that.schema)
        && Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, values);
  }

  @Override
  public String toString() {
    return "KaypherStruct{"
        + "values=" + values
        + ", schema=" + schema
        + '}';
  }

  private static FieldInfo getField(final String name, final SqlStruct schema) {
    final List<Field> fields = schema.fields();

    for (int idx = 0; idx < fields.size(); idx++) {
      final Field field = fields.get(idx);
      if (field.name().equals(name)) {
        return new FieldInfo(idx, field);
      }
    }

    throw new KaypherException("Unknown field: " + name);
  }

  public static final class Builder {

    private final SqlStruct schema;
    private final List<Optional<?>> values;

    public Builder(final SqlStruct schema) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.values = new ArrayList<>(schema.fields().size());
      schema.fields().forEach(f -> values.add(Optional.empty()));
    }

    public Builder set(final String field, final Optional<?> value) {
      final FieldInfo info = getField(field, schema);
      info.field.type().validateValue(value.orElse(null));
      values.set(info.index, value);
      return this;
    }

    public KaypherStruct build() {
      return new KaypherStruct(schema, values);
    }
  }

  private static final class FieldInfo {

    final int index;
    final Field field;

    private FieldInfo(final int index, final Field field) {
      this.index = index;
      this.field = Objects.requireNonNull(field, "field");
    }
  }
}
