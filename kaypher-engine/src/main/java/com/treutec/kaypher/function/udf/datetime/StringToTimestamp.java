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
package com.treutec.kaypher.function.udf.datetime;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.treutec.kaypher.function.KaypherFunctionException;
import com.treutec.kaypher.function.udf.Udf;
import com.treutec.kaypher.function.udf.UdfDescription;
import com.treutec.kaypher.function.udf.UdfParameter;
import com.treutec.kaypher.util.KaypherConstants;
import com.treutec.kaypher.util.timestamp.StringToTimestampParser;

import java.time.ZoneId;
import java.util.concurrent.ExecutionException;

@UdfDescription(name = "stringtotimestamp", author = KaypherConstants.CONFLUENT_AUTHOR,
    description = "Converts a string representation of a date in the given format"
        + " into the BIGINT value that represents the millisecond timestamp.")
public class StringToTimestamp {

  private final LoadingCache<String, StringToTimestampParser> parsers =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(StringToTimestampParser::new));

  @Udf(description = "Converts a string representation of a date in the given format"
      + " into the BIGINT value that represents the millisecond timestamp."
      + " Single quotes in the timestamp format can be escaped with '',"
      + " for example: 'yyyy-MM-dd''T''HH:mm:ssX'.")
  public long stringToTimestamp(
      @UdfParameter(
          description = "The string representation of a date.") final String formattedTimestamp,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
    try {
      final StringToTimestampParser timestampParser = parsers.get(formatPattern);
      return timestampParser.parse(formattedTimestamp);
    } catch (final ExecutionException | RuntimeException e) {
      throw new KaypherFunctionException("Failed to parse timestamp '" + formattedTimestamp
           + "' with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }

  @Udf(description = "Converts a string representation of a date at the given time zone"
      + " in the given format into the BIGINT value that represents the millisecond timestamp."
      + " Single quotes in the timestamp format can be escaped with '',"
      + " for example: 'yyyy-MM-dd''T''HH:mm:ssX'.")
  public long stringToTimestamp(
      @UdfParameter(
          description = "The string representation of a date.") final String formattedTimestamp,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern,
      @UdfParameter(
          description =  " timeZone is a java.util.TimeZone ID format, for example: \"UTC\","
              + " \"America/Los_Angeles\", \"PST\", \"Europe/London\"") final String timeZone) {
    try {
      final StringToTimestampParser timestampParser = parsers.get(formatPattern);
      final ZoneId zoneId = ZoneId.of(timeZone);
      return timestampParser.parse(formattedTimestamp, zoneId);
    } catch (final ExecutionException | RuntimeException e) {
      throw new KaypherFunctionException("Failed to parse timestamp '" + formattedTimestamp
          + "' at timezone '" + timeZone + "' with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }

}
