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

package com.treutec.kaypher.metastore;

import com.google.common.collect.Iterators;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

/**
 * {@code TypeRegistry} maintains a mapping from custom schema names
 * to more complicated schemas.
 */
public interface TypeRegistry {

  /**
   * Registers a custom type name with a specified schema
   *
   * @param name   the name, must be unique
   * @param type   the schema to associate it with
   */
  void registerType(String name, SqlType type);

  /**
   * @param name the previously registered name
   * @return whether or not a type was dropped
   */
  boolean deleteType(String name);

  /**
   * Resolves a custom type name to a previously registered type
   *
   * @param name the custom type name
   * @return the type it was registered with, or {@link Optional#empty()} if
   *         there was no custom type with this name registered
   */
  Optional<SqlType> resolveType(String name);

  /**
   * @return an iterable of all types registered in this registry
   */
  Iterator<CustomType> types();

  class CustomType {
    private final String name;
    private final SqlType type;

    public CustomType(final String name, final SqlType type) {
      this.name = Objects.requireNonNull(name, "name");
      this.type = Objects.requireNonNull(type, "type");
    }

    public String getName() {
      return name;
    }

    public SqlType getType() {
      return type;
    }
  }

  /**
   * An empty type registry that does not support registering or deleting types.
   */
  TypeRegistry EMPTY = new TypeRegistry() {
    @Override
    public void registerType(final String name, final SqlType type) { }

    @Override
    public boolean deleteType(final String name) {
      return false;
    }

    @Override
    public Optional<SqlType> resolveType(final String name) {
      return Optional.empty();
    }

    @Override
    public Iterator<CustomType> types() {
      return Iterators.forArray();
    }
  };

}
