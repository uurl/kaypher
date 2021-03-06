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
package com.treutec.kaypher.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import org.junit.BeforeClass;
import org.junit.Test;

public class LogicalSchemaSerializerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeClass
  public static void classSetUp() {
    MAPPER.registerModule(new TestModule());
  }

  @Test
  public void shouldSerializeSchemaWithImplicitColumns() throws Exception {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .build();

    // When:
    final String json = MAPPER.writeValueAsString(schema);

    // Then:
    assertThat(json, is("\"`ROWKEY` STRING KEY, `v0` INTEGER\""));
  }

  @Test
  public void shouldSerializeSchemaWithOutImplicitColumns() throws Exception {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .noImplicitColumns()
        .keyColumn(ColumnName.of("key0"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .build();

    // When:
    final String json = MAPPER.writeValueAsString(schema);

    // Then:
    assertThat(json, is("\"`key0` STRING KEY, `v0` INTEGER\""));
  }

  @Test
  public void shouldSerializeSchemaWithKeyAfterValue() throws Exception {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .keyColumn(ColumnName.of("key0"), SqlTypes.STRING)
        .build();

    // When:
    final String json = MAPPER.writeValueAsString(schema);

    // Then:
    assertThat(json, is("\"`v0` INTEGER, `key0` STRING KEY\""));
  }

  private static final class TestModule extends SimpleModule {

    TestModule() {
      addSerializer(LogicalSchema.class, new LogicalSchemaSerializer());
    }
  }
}