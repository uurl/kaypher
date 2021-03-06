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

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import com.treutec.kaypher.parser.tree.SetProperty;
import com.treutec.kaypher.parser.tree.UnsetProperty;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.util.KaypherStatementException;
import java.util.Map;

public final class PropertyOverrider {

  private PropertyOverrider() { }

  public static void set(
      final ConfiguredStatement<SetProperty> statement,
      final Map<String, Object> mutableProperties
  ) {
    final SetProperty setProperty = statement.getStatement();
    throwIfInvalidProperty(setProperty.getPropertyName(), statement.getStatementText());
    throwIfInvalidPropertyValues(setProperty, statement);
    mutableProperties.put(setProperty.getPropertyName(), setProperty.getPropertyValue());
  }

  public static void unset(
      final ConfiguredStatement<UnsetProperty> statement,
      final Map<String, Object> mutableProperties
  ) {
    final UnsetProperty unsetProperty = statement.getStatement();
    throwIfInvalidProperty(unsetProperty.getPropertyName(), statement.getStatementText());
    mutableProperties.remove(unsetProperty.getPropertyName());
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED") // clone has side-effects
  private static void throwIfInvalidPropertyValues(
      final SetProperty setProperty,
      final ConfiguredStatement<SetProperty> statement) {
    try {
      statement.getConfig().cloneWithPropertyOverwrite(ImmutableMap.of(
          setProperty.getPropertyName(),
          setProperty.getPropertyValue()
      ));
    } catch (final Exception e) {
      throw new KaypherStatementException(
          e.getMessage(), statement.getStatementText(), e.getCause());
    }
  }

  private static void throwIfInvalidProperty(final String propertyName, final String text) {
    if (!LocalPropertyValidator.CONFIG_PROPERTY_WHITELIST.contains(propertyName)) {
      throw new KaypherStatementException("Unknown property: " + propertyName, text);
    }
  }

}
