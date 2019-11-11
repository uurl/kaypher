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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.execution.expression.tree.IntegerLiteral;
import com.treutec.kaypher.execution.expression.tree.Literal;
import com.treutec.kaypher.execution.expression.tree.StringLiteral;
import com.treutec.kaypher.parser.ColumnReferenceParser;
import com.treutec.kaypher.properties.with.CommonCreateConfigs;
import com.treutec.kaypher.properties.with.CreateAsConfigs;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.serde.Delimiter;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.util.KaypherException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigException;

/**
 * Performs validation of a CREATE AS statement's WITH clause.
 */
@Immutable
public final class CreateSourceAsProperties {

  private final PropertiesConfig props;

  public static CreateSourceAsProperties none() {
    return new CreateSourceAsProperties(ImmutableMap.of());
  }

  public static CreateSourceAsProperties from(final Map<String, Literal> literals) {
    try {
      return new CreateSourceAsProperties(literals);
    } catch (final ConfigException e) {
      final String message = e.getMessage().replace(
          "configuration",
          "property"
      );

      throw new KaypherException(message, e);
    }
  }

  private CreateSourceAsProperties(final Map<String, Literal> originals) {
    this.props = new PropertiesConfig(CreateAsConfigs.CONFIG_METADATA, originals);

    props.validateDateTimeFormat(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY);
  }

  public Optional<Format> getValueFormat() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.VALUE_FORMAT_PROPERTY))
        .map(String::toUpperCase)
        .map(Format::valueOf);
  }

  public Optional<String> getKafkaTopic() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY));
  }

  public Optional<Integer> getPartitions() {
    return Optional.ofNullable(props.getInt(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS));
  }

  public Optional<Short> getReplicas() {
    return Optional.ofNullable(props.getShort(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS));
  }


  public Optional<ColumnRef> getTimestampColumnName() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY))
        .map(ColumnReferenceParser::parse);
  }

  public Optional<String> getTimestampFormat() {
    return Optional.ofNullable(props.getString(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY));
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

  public CreateSourceAsProperties withTopic(
      final String name,
      final int partitions,
      final short replicas
  ) {
    final Map<String, Literal> originals = props.copyOfOriginalLiterals();
    originals.put(CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(name));
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS, new IntegerLiteral(partitions));
    originals.put(CommonCreateConfigs.SOURCE_NUMBER_OF_REPLICAS, new IntegerLiteral(replicas));

    return new CreateSourceAsProperties(originals);
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
    final CreateSourceAsProperties that = (CreateSourceAsProperties) o;
    return Objects.equals(props, that.props);
  }

  @Override
  public int hashCode() {
    return Objects.hash(props);
  }
}
