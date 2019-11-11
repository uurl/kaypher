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

package com.treutec.kaypher.serde.delimited;

import com.google.errorprone.annotations.Immutable;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.connect.SchemaWalker;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.Delimiter;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KaypherSerdeFactory;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KaypherConfig;
import io.confluent.ksql.util.KaypherException;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;


@Immutable
public class KaypherDelimitedSerdeFactory implements KaypherSerdeFactory {

  private static final Delimiter DEFAULT_DELIMITER = Delimiter.of(',');

  private final CSVFormat csvFormat;

  public KaypherDelimitedSerdeFactory(final Optional<Delimiter> delimiter) {
    this.csvFormat =
        CSVFormat.DEFAULT.withDelimiter(delimiter.orElse(DEFAULT_DELIMITER).getDelimiter());
  }

  @Override
  public void validate(final PersistenceSchema schema) {
    final ConnectSchema connectSchema = schema.serializedSchema();
    if (connectSchema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("DELIMITED format does not support unwrapping");
    }

    connectSchema.fields().forEach(f -> SchemaWalker.visit(f.schema(), new SchemaValidator()));
  }

  @Override
  public Serde<Object> createSerde(
      final PersistenceSchema schema,
      final KaypherConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    validate(schema);

    return Serdes.serdeFrom(
        new KaypherDelimitedSerializer(csvFormat),
        new KaypherDelimitedDeserializer(schema, csvFormat)
    );
  }

  private static class SchemaValidator implements SchemaWalker.Visitor<Void, Void> {

    public Void visitPrimitive(final Schema schema) {
      // Primitive types are allowed.
      return null;
    }

    public Void visitBytes(final Schema schema) {
      if (!DecimalUtil.isDecimal(schema)) {
        visitSchema(schema);
      }
      return null;
    }

    public Void visitSchema(final Schema schema) {
      throw new KaypherException("The '" + Format.DELIMITED
          + "' format does not support type '" + schema.type().toString() + "'");
    }
  }
}
