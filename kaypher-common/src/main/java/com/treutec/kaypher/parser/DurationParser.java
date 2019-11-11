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

package com.treutec.kaypher.parser;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

/**
 * Parser for duration strings, e.g. '10 SECONDS'.
 */
public final class DurationParser {

  private DurationParser() {
  }

  public static Duration parse(final String text) {
    try {
      final String[] parts = text.split("\\s");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Expected 2 tokens, got: " + parts.length);
      }

      final long size = parseNumeric(parts[0]);
      final TimeUnit timeUnit = parseTimeUnit(parts[1].toUpperCase());

      return Duration.ofNanos(timeUnit.toNanos(size));
    } catch (final Exception e) {
      throw new IllegalArgumentException("Invalid duration: '" + text + "'. " + e.getMessage(), e);
    }
  }

  private static long parseNumeric(final String text) {
    try {
      return Long.parseLong(text);
    } catch (final Exception e) {
      throw new IllegalArgumentException("Not numeric: '" + text + "'");
    }
  }

  private static TimeUnit parseTimeUnit(final String text) {
    try {
      final String timeUnit = text.endsWith("S")
          ? text
          : text + "S";

      return TimeUnit.valueOf(timeUnit);
    } catch (final Exception e) {
      throw new IllegalArgumentException("Unknown time unit: '" + text + "'"
          + System.lineSeparator()
          + "Supported time units are: " + StringUtils.join(TimeUnit.values(), ", "));
    }
  }
}
