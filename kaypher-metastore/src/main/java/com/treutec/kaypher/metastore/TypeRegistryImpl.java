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

import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.util.KaypherException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class TypeRegistryImpl implements TypeRegistry {

  private final Map<String, SqlType> typeRegistry = new ConcurrentHashMap<>();

  @Override
  public void registerType(final String name, final SqlType type) {
    final SqlType oldValue = typeRegistry.putIfAbsent(name.toUpperCase(), type);
    if (oldValue != null) {
      throw new KaypherException(
          "Cannot register custom type '" + name + "' "
              + "since it is already registered with type: " + type
      );
    }
  }

  @Override
  public boolean deleteType(final String name) {
    return typeRegistry.remove(name.toUpperCase()) != null;
  }

  @Override
  public Optional<SqlType> resolveType(final String name) {
    return Optional.ofNullable(typeRegistry.get(name.toUpperCase()));
  }

  @Override
  public Iterator<CustomType> types() {
    return typeRegistry
        .entrySet()
        .stream()
        .map(kv -> new CustomType(kv.getKey(), kv.getValue())).iterator();
  }

}
