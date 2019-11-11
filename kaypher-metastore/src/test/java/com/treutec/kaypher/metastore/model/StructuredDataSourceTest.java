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
package com.treutec.kaypher.metastore.model;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.util.SchemaUtil;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StructuredDataSourceTest {

  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
      .build();

  @Mock
  public KeyField keyField;

  @Test
  public void shouldValidateKeyFieldIsInSchema() {
    // When:
    new TestStructuredDataSource(
        SOME_SCHEMA,
        keyField
    );

    // Then (no exception):
    verify(keyField).validateKeyExistsIn(SOME_SCHEMA);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsRowTime() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(
        schema,
        keyField
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsRowKey() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(
        schema,
        keyField
    );
  }

  /**
   * Test class to allow the abstract base class to be instantiated.
   */
  private static final class TestStructuredDataSource extends StructuredDataSource<String> {

    private TestStructuredDataSource(
        final LogicalSchema schema,
        final KeyField keyField
    ) {
      super(
          "some SQL",
          SourceName.of("some name"),
          schema,
          SerdeOption.none(), keyField,
          mock(TimestampExtractionPolicy.class),
          DataSourceType.KSTREAM,
          mock(KaypherTopic.class)
      );
    }
  }
}