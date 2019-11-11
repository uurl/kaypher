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


package com.treutec.kaypher.model;

import org.apache.commons.lang3.StringUtils;

/**
 * Enum of the windowing types KAYPHER supports.
 */
public enum WindowType {

  SESSION(false),
  HOPPING(true),
  TUMBLING(true);

  private final boolean requiresWindowSize;

  WindowType(final boolean requiresWindowSize) {
    this.requiresWindowSize = requiresWindowSize;
  }

  public boolean requiresWindowSize() {
    return requiresWindowSize;
  }

  public static WindowType of(final String text) {
    try {
      return WindowType.valueOf(text.toUpperCase());
    } catch (final Exception e) {
      throw new IllegalArgumentException("Unknown window type: '" + text + "' "
          + System.lineSeparator()
          + "Valid values are: " + StringUtils.join(WindowType.values(), ", "),
          e);
    }
  }
}
