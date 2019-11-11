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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings("unchecked")
public class ConnectDataTranslatorTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldTranslateStructCorrectly() {
    // Given:
    final Schema structSchema = SchemaBuilder
        .struct()
        .field("INT", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .field("BIGINT", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .optional()
        .build();
    final Schema rowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", structSchema)
        .optional()
        .build();

    final Struct connectStruct = new Struct(rowSchema);
    final Struct structColumn = new Struct(structSchema);
    structColumn.put("INT", 123);
    structColumn.put("BIGINT", 456L);
    connectStruct.put("STRUCT", structColumn);

    final ConnectDataTranslator connectToKaypherTranslator = new ConnectDataTranslator(rowSchema);

    // When:
    final Struct row = (Struct) connectToKaypherTranslator.toKaypherRow(rowSchema, connectStruct);

    // Then:
    assertThat(row.schema(), is(rowSchema));
    assertThat(row.get("STRUCT"), instanceOf(Struct.class));
    final Struct connectStructColumn = (Struct)row.get("STRUCT");
    assertThat(connectStructColumn.schema(), equalTo(structSchema));
    assertThat(connectStructColumn.get("INT"), equalTo(123));
    assertThat(connectStructColumn.get("BIGINT"), equalTo(456L));
  }

  @Test
  public void shouldTranslateArrayOfStructs() {
    // Given:
    final Schema innerSchema = SchemaBuilder
        .struct()
        .field("FIELD", Schema.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    final Schema rowSchema = SchemaBuilder
        .struct()
        .field("ARRAY", SchemaBuilder
            .array(innerSchema)
            .optional()
            .build())
        .build();

    final Struct connectStruct = new Struct(rowSchema);
    final Struct inner1 = new Struct(innerSchema);
    inner1.put("FIELD", 123);
    final Struct inner2 = new Struct(innerSchema);
    inner2.put("FIELD", 456);
    connectStruct.put("ARRAY", Arrays.asList(inner1, inner2));

    final ConnectDataTranslator connectToKaypherTranslator = new ConnectDataTranslator(rowSchema);

    // When:
    final Struct row = (Struct) connectToKaypherTranslator.toKaypherRow(rowSchema, connectStruct);

    // Then:
    assertThat(row.get("ARRAY"), instanceOf(List.class));

    final List<Struct> array = (List<Struct>)row.get("ARRAY");
    assertThat(array.get(0).get("FIELD"), equalTo(123));
    assertThat(array.get(1).get("FIELD"), equalTo(456));
  }

  @Test
  public void shouldTranslateMapWithStructValues() {
    // Given:
    final Schema innerSchema = SchemaBuilder
            .struct()
            .field("FIELD", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    final Schema rowSchema = SchemaBuilder
        .struct()
        .field("MAP", SchemaBuilder
            .map(Schema.STRING_SCHEMA, innerSchema)
            .optional()
            .build()
        ).build();

    final Struct connectStruct = new Struct(rowSchema);
    final Struct inner1 = new Struct(innerSchema);
    inner1.put("FIELD", 123);
    final Struct inner2 = new Struct(innerSchema);
    inner2.put("FIELD", 456);
    connectStruct.put("MAP", ImmutableMap.of("k1", inner1, "k2", inner2));

    final ConnectDataTranslator connectToKaypherTranslator = new ConnectDataTranslator(rowSchema);

    // When:
    final Struct row = (Struct) connectToKaypherTranslator.toKaypherRow(rowSchema, connectStruct);


    assertThat(row.get("MAP"), instanceOf(Map.class));

    final Map<String, Struct> map = (Map<String, Struct>)row.get("MAP");
    assertThat(map.get("k1").get("FIELD"), equalTo(123));
    assertThat(map.get("k2").get("FIELD"), equalTo(456));
  }

  @Test
  public void shouldThrowOnTypeMismatch() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("FIELD", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    final Schema badSchema = SchemaBuilder.struct()
        .field("FIELD", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();

    final Struct badData = new Struct(badSchema);
    badData.put("FIELD", "fubar");

    final ConnectDataTranslator connectToKaypherTranslator = new ConnectDataTranslator(schema);

    // Then:
    expectedException.expect(DataException.class);
    expectedException.expectMessage(Schema.Type.STRING.getName());
    expectedException.expectMessage(Schema.Type.INT32.getName());
    expectedException.expectMessage("FIELD");

    // When:
    connectToKaypherTranslator.toKaypherRow(badSchema, badData);
  }

  @Test
  public void shouldTranslateStructFieldWithDifferentCase() {
    // Given:
    final Schema structSchema = SchemaBuilder
        .struct()
        .field("INT", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    final Schema rowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", structSchema)
        .build();

    final Schema dataStructSchema = SchemaBuilder
        .struct()
        .field("iNt", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    final Schema dataRowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", dataStructSchema)
        .optional()
        .build();

    final Struct connectStruct = new Struct(dataRowSchema);
    final Struct structColumn = new Struct(dataStructSchema);
    structColumn.put("iNt", 123);
    connectStruct.put("STRUCT", structColumn);

    final ConnectDataTranslator connectToKaypherTranslator = new ConnectDataTranslator(rowSchema);

    // When:
    final Struct row = (Struct) connectToKaypherTranslator.toKaypherRow(dataRowSchema, connectStruct);

    // Then:
    assertThat(row.get("STRUCT"), instanceOf(Struct.class));
    final Struct connectStructColumn = (Struct)row.get("STRUCT");
    assertThat(connectStructColumn.schema(), equalTo(structSchema));
    assertThat(connectStructColumn.get("INT"), equalTo(123));
  }

  @Test
  public void shouldThrowIfNestedFieldTypeDoesntMatch() {
    // Given:
    final Schema structSchema = SchemaBuilder
        .struct()
        .field("INT", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    final Schema rowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", structSchema)
        .optional()
        .build();

    final Schema dataStructSchema = SchemaBuilder
        .struct()
        .field("INT", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .optional()
        .build();

    final Schema dataRowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", dataStructSchema)
        .optional()
        .build();

    final Struct connectStruct = new Struct(dataRowSchema);
    final Struct structColumn = new Struct(dataStructSchema);
    structColumn.put("INT", "123");
    connectStruct.put("STRUCT", structColumn);

    final ConnectDataTranslator connectToKaypherTranslator = new ConnectDataTranslator(rowSchema);

    // Then:
    expectedException.expect(DataException.class);
    expectedException.expectMessage(Schema.Type.INT32.getName());
    expectedException.expectMessage(Schema.Type.STRING.getName());
    expectedException.expectMessage("STRUCT->INT");

    // When:
    connectToKaypherTranslator.toKaypherRow(dataRowSchema, connectStruct);
  }

  @Test
  public void shouldTranslateNullValueCorrectly() {
    // Given:
    final Schema rowSchema = SchemaBuilder.struct()
        .field("INT", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    final Struct connectStruct = new Struct(rowSchema);

    final ConnectDataTranslator connectToKaypherTranslator = new ConnectDataTranslator(rowSchema);

    // When:
    final Struct row = (Struct) connectToKaypherTranslator.toKaypherRow(rowSchema, connectStruct);

    // Then:
    assertThat(row.get("INT"), is(nullValue()));
  }

  @Test
  public void shouldTranslateMissingStructFieldToNull() {
    // Given:
    final Schema structSchema = SchemaBuilder
        .struct()
        .field("INT", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    final Schema rowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", structSchema)
        .optional()
        .build();

    final Schema dataRowSchema = SchemaBuilder
        .struct()
        .field("OTHER", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    final Struct connectStruct = new Struct(dataRowSchema);
    connectStruct.put("OTHER", 123);

    final ConnectDataTranslator connectToKaypherTranslator = new ConnectDataTranslator(rowSchema);

    // When:
    final Struct row = (Struct) connectToKaypherTranslator.toKaypherRow(dataRowSchema, connectStruct);

    // Then:
    assertThat(row.schema(), is(rowSchema));
    assertThat(row.get("STRUCT"), is(nullValue()));
  }
}
