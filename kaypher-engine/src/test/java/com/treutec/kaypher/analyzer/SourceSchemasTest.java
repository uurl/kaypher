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
package com.treutec.kaypher.analyzer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;

public class SourceSchemasTest {

  private static final SourceName ALIAS_1 = SourceName.of("S1");
  private static final SourceName ALIAS_2 = SourceName.of("S2");
  private static final ColumnName COMMON_FIELD_NAME = ColumnName.of("F0");

  private static final LogicalSchema SCHEMA_1 = LogicalSchema.builder()
      .valueColumn(COMMON_FIELD_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("F1"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema SCHEMA_2 = LogicalSchema.builder()
      .valueColumn(COMMON_FIELD_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("F2"), SqlTypes.STRING)
      .build();

  private SourceSchemas sourceSchemas;

  @Before
  public void setUp() {
    sourceSchemas = new SourceSchemas(ImmutableMap.of(ALIAS_1, SCHEMA_1, ALIAS_2, SCHEMA_2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnNoSchemas() {
    new SourceSchemas(ImmutableMap.of());
  }

  @Test
  public void shouldNotBeJoinIfSingleSchema() {
    // When:
    sourceSchemas = new SourceSchemas(ImmutableMap.of(ALIAS_1, SCHEMA_1));

    // Then:
    assertThat(sourceSchemas.isJoin(), is(false));
  }

  @Test
  public void shouldBeJoinIfMultipleSchemas() {
    // When:
    sourceSchemas = new SourceSchemas(ImmutableMap.of(ALIAS_1, SCHEMA_1, ALIAS_2, SCHEMA_2));

    // Then:
    assertThat(sourceSchemas.isJoin(), is(true));
  }

  @Test
  public void shouldFindNoField() {
    assertThat(sourceSchemas.sourcesWithField(ColumnRef.withoutSource(ColumnName.of("unknown"))), is(empty()));
  }

  @Test
  public void shouldFindNoQualifiedField() {
    assertThat(sourceSchemas.sourcesWithField(ColumnRef.of(ALIAS_1, ColumnName.of("F2"))), is(empty()));
  }

  @Test
  public void shouldFindUnqualifiedUniqueField() {
    assertThat(sourceSchemas.sourcesWithField(ColumnRef.withoutSource(ColumnName.of("F1"))), contains(ALIAS_1));
  }

  @Test
  public void shouldFindQualifiedUniqueField() {
    assertThat(sourceSchemas.sourcesWithField(ColumnRef.of(ALIAS_2, ColumnName.of("F2"))), contains(ALIAS_2));
  }

  @Test
  public void shouldFindUnqualifiedCommonField() {
    assertThat(sourceSchemas.sourcesWithField(ColumnRef.withoutSource(COMMON_FIELD_NAME)),
        containsInAnyOrder(ALIAS_1, ALIAS_2));
  }

  @Test
  public void shouldFindQualifiedFieldOnlyInThatSource() {
    assertThat(sourceSchemas.sourcesWithField(ColumnRef.of(ALIAS_1, COMMON_FIELD_NAME)),
        contains(ALIAS_1));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfMetaField() {
    assertThat(sourceSchemas.matchesNonValueField(ColumnRef.withoutSource(SchemaUtil.ROWTIME_NAME)), is(true));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfAliaasedMetaField() {
    assertThat(sourceSchemas.matchesNonValueField(ColumnRef.of(ALIAS_2, SchemaUtil.ROWTIME_NAME)), is(true));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfKeyField() {
    assertThat(sourceSchemas.matchesNonValueField(ColumnRef.withoutSource(SchemaUtil.ROWKEY_NAME)), is(true));
  }

  @Test
  public void shouldMatchNonValueFieldNameIfAliasedKeyField() {
    assertThat(sourceSchemas.matchesNonValueField(ColumnRef.of(ALIAS_2, SchemaUtil.ROWKEY_NAME)), is(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnUnknownSourceWhenMatchingNonValueFields() {
    sourceSchemas.matchesNonValueField(ColumnRef.of(SourceName.of("unknown"), SchemaUtil.ROWKEY_NAME));
  }

  @Test
  public void shouldNotMatchOtherFields() {
    assertThat(sourceSchemas.matchesNonValueField(ColumnRef.of(ALIAS_2, ColumnName.of("F2"))), is(false));
  }

  @Test
  public void shouldNotMatchUnknownFields() {
    assertThat(sourceSchemas.matchesNonValueField(ColumnRef.withoutSource(ColumnName.of("unknown"))), is(false));
  }
}
