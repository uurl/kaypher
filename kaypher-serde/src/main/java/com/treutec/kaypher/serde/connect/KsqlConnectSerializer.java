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

package com.treutec.kaypher.serde.connect;

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.storage.Converter;

public class KaypherConnectSerializer implements Serializer<Object> {

  private final Schema schema;
  private final DataTranslator translator;
  private final Converter converter;

  public KaypherConnectSerializer(
      final Schema schema,
      final DataTranslator translator,
      final Converter converter
  ) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.translator = Objects.requireNonNull(translator, "translator");
    this.converter = Objects.requireNonNull(converter, "converter");
  }

  @Override
  public byte[] serialize(final String topic, final Object data) {
    if (data == null) {
      return null;
    }

    try {
      final Object connectRow = translator.toConnectRow(data);
      return converter.fromConnectData(topic, schema, connectRow);
    } catch (final Exception e) {
      throw new SerializationException(
          "Error serializing message to topic: " + topic, e);
    }
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public void close() {
  }
}
