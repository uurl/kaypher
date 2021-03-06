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

package com.treutec.kaypher.util.timestamp;

import com.google.common.annotations.VisibleForTesting;
import com.treutec.kaypher.util.KaypherException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.util.Locale;
import java.util.function.Function;
import org.apache.commons.lang3.ObjectUtils;

public class StringToTimestampParser {

  private static final Function<ZoneId, ZonedDateTime> DEFAULT_ZONED_DATE_TIME =
      zid -> ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, zid);

  private final DateTimeFormatter formatter;

  public StringToTimestampParser(final String pattern) {
    formatter = DateTimeFormatter.ofPattern(pattern, Locale.ROOT);
  }

  /**
   * Parse with a default time zone of {@code ZoneId#systemDefault}
   *
   * @see #parse(String, ZoneId)
   */
  public long parse(final String text) {
    return parse(text, ZoneId.systemDefault());
  }

  /**
   * @param text    the textual representation of the timestamp
   * @param zoneId  the zoneId to use, if none present in {@code text}
   *
   * @return the millis since epoch that {@code text} represents
   */
  public long parse(final String text, final ZoneId zoneId) {
    return parseZoned(text, zoneId).toInstant().toEpochMilli();
  }

  @VisibleForTesting
  ZonedDateTime parseZoned(final String text, final ZoneId zoneId) {
    final TemporalAccessor parsed = formatter.parse(text);
    final ZoneId parsedZone = parsed.query(TemporalQueries.zone());

    ZonedDateTime resolved = DEFAULT_ZONED_DATE_TIME.apply(
        ObjectUtils.defaultIfNull(parsedZone, zoneId));

    for (final TemporalField override : ChronoField.values()) {
      if (parsed.isSupported(override)) {
        if (!resolved.isSupported(override)) {
          throw new KaypherException(
              "Unsupported temporal field in timestamp: " + text + " (" + override + ")");
        }
        resolved = resolved.with(override, parsed.getLong(override));
      }
    }

    return resolved;
  }

}
