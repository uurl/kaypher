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

import com.treutec.kaypher.properties.with.CommonCreateConfigs;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import java.util.Optional;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;

public final class TimestampExtractionPolicyFactory {

  private TimestampExtractionPolicyFactory() {
  }

  public static TimestampExtractionPolicy create(
      final KaypherConfig kaypherConfig,
      final LogicalSchema schema,
      final Optional<ColumnRef> timestampColumnName,
      final Optional<String> timestampFormat
  ) {
    if (!timestampColumnName.isPresent()) {
      return new MetadataTimestampExtractionPolicy(getDefaultTimestampExtractor(kaypherConfig));
    }

    final ColumnRef col = timestampColumnName.get();

    final Column timestampColumn = schema.findValueColumn(col)
        .orElseThrow(() -> new KaypherException(
            "The TIMESTAMP column set in the WITH clause does not exist in the schema: '"
                + col.toString(FormatOptions.noEscape()) + "'"));

    final SqlBaseType tsColumnType = timestampColumn.type().baseType();
    if (tsColumnType == SqlBaseType.STRING) {

      final String format = timestampFormat.orElseThrow(() -> new KaypherException(
          "A String timestamp field has been specified without"
              + " also specifying the "
              + CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY.toLowerCase()));

      return new StringTimestampExtractionPolicy(col, format);
    }

    if (timestampFormat.isPresent()) {
      throw new KaypherException("'" + CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY
          + "' set in the WITH clause can only be used "
          + "when the timestamp column in of type STRING.");
    }

    if (tsColumnType == SqlBaseType.BIGINT) {
      return new LongColumnTimestampExtractionPolicy(col);
    }

    throw new KaypherException(
        "Timestamp column, " + timestampColumnName + ", should be LONG(INT64)"
            + " or a String with a "
            + CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY.toLowerCase()
            + " specified");
  }

  private static TimestampExtractor getDefaultTimestampExtractor(final KaypherConfig kaypherConfig) {
    try {
      final Class<?> timestampExtractorClass = (Class<?>) kaypherConfig.getKaypherStreamConfigProps()
          .getOrDefault(
              StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
              FailOnInvalidTimestamp.class
          );

      return (TimestampExtractor) timestampExtractorClass.newInstance();
    } catch (final Exception e) {
      throw new KaypherException("Cannot override default timestamp extractor: " + e.getMessage(), e);
    }
  }
}
