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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;

@UdfDescription(name = "stringtodate", author = KaypherConstants.CONFLUENT_AUTHOR,
    description = "Converts a string representation of a date into an integer representing"
        + " days since epoch using the given format pattern."
        + " Note this is the format Kafka Connect uses to represent dates with no time component."
        + " The format pattern should be in the format expected by"
        + " java.time.format.DateTimeFormatter")
public class StringToDate {

  private final LoadingCache<String, DateTimeFormatter> formatters =
      CacheBuilder.newBuilder()
          .maximumSize(1000)
          .build(CacheLoader.from(DateTimeFormatter::ofPattern));

  @Udf(description = "Converts formattedDate, a string representation of a date into"
      + " an integer representing days since epoch using the given formatPattern.")
  public int stringToDate(
      @UdfParameter(
          description = "The string representation of a date.") final String formattedDate,
      @UdfParameter(
          description = "The format pattern should be in the format expected by"
              + " java.time.format.DateTimeFormatter.") final String formatPattern) {
    try {
      final DateTimeFormatter formatter = formatters.get(formatPattern);
      return ((int)LocalDate.parse(formattedDate, formatter).toEpochDay());
    } catch (final ExecutionException | RuntimeException e) {
      throw new KaypherFunctionException("Failed to parse date '" + formattedDate
          + "' with formatter '" + formatPattern
          + "': " + e.getMessage(), e);
    }
  }

}