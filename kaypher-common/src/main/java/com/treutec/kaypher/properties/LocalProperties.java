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

package com.treutec.kaypher.properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.config.PropertyParser;
import com.treutec.kaypher.util.KaypherException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class LocalProperties {

  private final Map<String, Object> props = new HashMap<>();
  private final PropertyParser parser;

  public LocalProperties(final Map<String, ?> initial) {
    this(initial, new LocalPropertyParser());
  }

  @VisibleForTesting
  LocalProperties(final Map<String, ?> initial, final PropertyParser parser) {
    this.parser = Objects.requireNonNull(parser, "parser");

    try {
      Objects.requireNonNull(initial, "initial").forEach(this::set);
    } catch (final Exception e) {
      throw new KaypherException("invalid property found: " + e.getMessage(), e);
    }
  }

  /**
   * Set property.
   *
   * @param property the name of the property
   * @param value the value for the property
   * @return the previous value for the property, or {@code null}.
   */
  public Object set(final String property, final Object value) {
    Objects.requireNonNull(value, "value");
    final Object parsed = parser.parse(property, value);
    return props.put(property, parsed);
  }

  /**
   * Unset a property.
   *
   * @param property the name of the property
   * @return the previous value for the property, or {@code null} if it was not set.
   */
  public Object unset(final String property) {
    return props.remove(property);
  }

  /**
   * @return an immutable Map of the currently set properties.
   */
  public Map<String, Object> toMap() {
    return ImmutableMap.copyOf(props);
  }
}
