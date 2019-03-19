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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import com.koneksys.kaypher.util.KaypherException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public final class Struct extends Type {

  private final ImmutableList<Field> fields;

  public static Builder builder() {
    return new Builder();
  }

  private Struct(final List<Field> fields) {
    super(Optional.empty(), SqlType.STRUCT);
    this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields"));
  }

  public List<Field> getFields() {
    return fields;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitStruct(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final Struct other = (Struct) obj;
    return Objects.equals(this.fields, other.fields);
  }

  @Immutable
  public static final class Field {

    private final String name;
    private final Type type;

    public Field(final String name, final Type type) {
      this.name = Objects.requireNonNull(name, "name");
      this.type = Objects.requireNonNull(type, "type");

      if (name.trim().isEmpty()) {
        throw new IllegalArgumentException("Name can not be empty");
      }
    }

    public String getName() {
      return name;
    }

    public Type getType() {
      return type;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Field field = (Field) o;
      return Objects.equals(name, field.name)
          && Objects.equals(type, field.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type);
    }

    @Override
    public String toString() {
      return name + " " + type;
    }
  }

  public static final class Builder {

    private final List<Field> fields = new ArrayList<>();

    public Builder addFields(final List<Field> fields) {
      fields.forEach(this::addField);
      return this;
    }

    public Builder addField(final String fieldName, final Type fieldType) {
      return addField(new Field(fieldName, fieldType));
    }

    private Builder addField(final Field field) {
      throwOnDuplicateFieldName(field);
      fields.add(field);
      return this;
    }

    public Struct build() {
      if (fields.isEmpty()) {
        throw new KaypherException("STRUCT type must define fields");
      }
      return new Struct(fields);
    }

    private void throwOnDuplicateFieldName(final Field other) {
      fields.stream()
          .filter(f -> f.getName().equals(other.getName()))
          .findAny()
          .ifPresent(duplicate -> {
            throw new KaypherException("Duplicate field names found in STRUCT: "
                + "'" + duplicate + "' and '" + other + "'");
          });
    }
  }
}
