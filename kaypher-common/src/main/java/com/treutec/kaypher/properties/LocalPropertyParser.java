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

import com.treutec.kaypher.config.ConfigItem;
import com.treutec.kaypher.config.ConfigResolver;
import com.treutec.kaypher.config.KaypherConfigResolver;
import com.treutec.kaypher.config.PropertyParser;
import com.treutec.kaypher.config.PropertyValidator;
import com.treutec.kaypher.util.KaypherConstants;
import java.util.Objects;

public class LocalPropertyParser implements PropertyParser {

  private final ConfigResolver resolver;
  private final PropertyValidator validator;

  public LocalPropertyParser() {
    this(new KaypherConfigResolver(), new LocalPropertyValidator());
  }

  LocalPropertyParser(final ConfigResolver resolver, final PropertyValidator validator) {
    this.resolver = Objects.requireNonNull(resolver, "resolver");
    this.validator = Objects.requireNonNull(validator, "validator");
  }

  @Override
  public Object parse(final String property, final Object value) {
    if (property.equalsIgnoreCase(KaypherConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT)) {
      validator.validate(property, value);
      return value;
    }

    final ConfigItem configItem = resolver.resolve(property, true)
        .orElseThrow(() -> new IllegalArgumentException(String.format(
            "Not recognizable as kaypher, streams, consumer, or producer property: '%s'", property)));

    final Object parsedValue = configItem.parseValue(value);

    validator.validate(configItem.getPropertyName(), parsedValue);
    return parsedValue;
  }
}
