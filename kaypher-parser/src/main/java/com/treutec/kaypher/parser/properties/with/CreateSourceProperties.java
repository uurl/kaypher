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
package com.treutec.kaypher.parser.properties.with;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.execution.expression.tree.IntegerLiteral;
import com.treutec.kaypher.execution.expression.tree.Literal;
import com.treutec.kaypher.model.WindowType;
import com.treutec.kaypher.parser.ColumnReferenceParser;
import com.treutec.kaypher.parser.DurationParser;
import com.treutec.kaypher.properties.with.CommonCreateConfigs;
import com.treutec.kaypher.properties.with.CreateConfigs;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.serde.Delimiter;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.util.KaypherException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.config.ConfigException;

/**
 * Performs validation of a CREATE statement's WITH clause.
 */
@Immutable
public final class CreateSourceProperties {

  private final PropertiesConfig props;
  private final transient Function<String, Duration> durationParser;

  public static CreateSourceProperties from(final Map<String, Literal> literals) {
    try {
      return new CreateSourceProperties(literals, DurationParser::parse);
    } catch (final ConfigException e) {
      final String message = e.getMessage().replace(
          "configuration",
          "property"
      );

      throw new KaypherException(message, e);
    }
  }

  @VisibleForTesting
  CreateSourceProperties(
      final Map<String, Literal> originals,
      final Function<String, Duration> durationParser
  ) {
    this.props = new PropertiesConfig(CreateConfigs.CONFIG_METADATA, originals);
    this.durationParser = Objects.requireNonNull(durationParser, "durationParser");

    props.validateDateTimeFormat(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY);
    validateWindowInfo();
  }

  public Format getValueFormat() {
    return Format.valueOf(props.getString(CommonCreateConfigs.VALUE_FORMAT_PROPERTY).toUpperCase());
  }

  public String getKafkaTopic() {
    return props.getString(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY);
  }

  public Optional<Integer> getPartitions() {
    return Optional.ofNullable(props.getInt(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS));
  }

  public Optional<Short> getReplicas() {
    return Optional.ofNullable(props.getShort(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS));
  }

  public Optional<ColumnRef> getKeyField() {
    return Optional.ofNullable(props.getString(CreateConfigs.KEY_NAME_PROPERTY))
        .map(ColumnReferenceParser::parse);
  }

  public Optional<WindowType> getWindowType() {
    try {
      return Optional.ofNullable(props.getString(CreateConfigs.WINDOW_TYPE_PROPERTY))
          .map(WindowType::of);
    } catch (final Exception e) {
      throw new KaypherException("Error in WITH clause property '"
          + CreateConfigs.WINDOW_TYPE_PROPERTY + "': " + e.getMessage(),
          e);
    }
  }

  public Optional<Duration> getWindowSize() {
    try {
      return Optional.ofNullable(props.getString(CreateConfigs.WINDOW_SIZE_PROPERTY))
          .map(durationParser);
    } catch (final Exception e) {
      throw new KaypherException("Error in WITH clause property '"
          + CreateConfigs.WINDOW_SIZE_PROPERTY + "': " + e.getMessage()
          + System.lineSeparator()
          + "Example valid value: '10 SECONDS'",
          e);
    }
  }

  public Optional<ColumnRef> getTimestampColumnName() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY))
        .map(ColumnReferenceParser::parse);
  }

  public Optional<String> getTimestampFormat() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY));
  }

  public Optional<Integer> getAvroSchemaId() {
    return Optional.ofNullable(props.getInt(CreateConfigs.AVRO_SCHEMA_ID));
  }

  public Optional<String> getValueAvroSchemaName() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME));
  }

  public Optional<Boolean> getWrapSingleValues() {
    return Optional.ofNullable(props.getBoolean(CommonCreateConfigs.WRAP_SINGLE_VALUE));
  }

  public Optional<Delimiter> getValueDelimiter() {
    final String val = props.getString(CommonCreateConfigs.VALUE_DELIMITER_PROPERTY);
    return val == null ? Optional.empty() : Optional.of(Delimiter.parse(val));
  }

  public CreateSourceProperties withSchemaId(final int id) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    originals.put(CreateConfigs.AVRO_SCHEMA_ID, new IntegerLiteral(id));

    return new CreateSourceProperties(originals, durationParser);
  }

  public CreateSourceProperties withPartitionsAndReplicas(
      final int partitions,
      final short replicas
  ) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS, new IntegerLiteral(replicas));

    return new CreateSourceProperties(originals, durationParser);
  }

  public Map<String, Literal> copyOfOriginalLiterals() {
    return props.copyOfOriginalLiterals();
  }

  @Override
  public String toString() {
    return props.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateSourceProperties that = (CreateSourceProperties) o;
    return Objects.equals(props, that.props);
  }

  @Override
  public int hashCode() {
    return Objects.hash(props);
  }

  private void validateWindowInfo() {
    final Optional<WindowType> windowType = getWindowType();
    final Optional<Duration> windowSize = getWindowSize();

    final boolean requiresSize = windowType.isPresent() && windowType.get() != WindowType.SESSION;

    if (requiresSize && !windowSize.isPresent()) {
      throw new KaypherException(windowType.get() + " windows require '"
          + CreateConfigs.WINDOW_SIZE_PROPERTY + "' to be provided in the WITH clause. "
          + "For example: '" + CreateConfigs.WINDOW_SIZE_PROPERTY + "'='10 SECONDS'");
    }

    if (!requiresSize && windowSize.isPresent()) {
      throw new KaypherException("'" + CreateConfigs.WINDOW_SIZE_PROPERTY + "' "
          + "should not be set for SESSION windows.");
    }
  }
}
