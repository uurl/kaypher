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

package com.treutec.kaypher.parser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;

import com.google.common.collect.Iterables;
import com.treutec.kaypher.execution.expression.tree.Type;
import com.treutec.kaypher.metastore.TypeRegistry;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.parser.tree.TableElement;
import com.treutec.kaypher.parser.tree.TableElement.Namespace;
import com.treutec.kaypher.parser.tree.TableElements;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.SchemaUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaParserTest {

  private static final ColumnName FOO = ColumnName.of("FOO");
  private static final ColumnName BAR = ColumnName.of("BAR");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private TypeRegistry typeRegistry;
  private SchemaParser parser;

  @Before
  public void setUp() {
    parser = new SchemaParser(typeRegistry);
  }

  @Test
  public void shouldParseValidSchema() {
    // Given:
    final String schema = "foo INTEGER, bar MAP<VARCHAR, VARCHAR>";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(elements, contains(
        new TableElement(Namespace.VALUE, FOO, new Type(SqlTypes.INTEGER)),
        new TableElement(Namespace.VALUE, BAR, new Type(SqlTypes.map(SqlTypes.STRING)))
    ));
  }

  @Test
  public void shouldParseValidSchemaWithKeyField() {
    // Given:
    final String schema = "ROWKEY STRING KEY, bar INT";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(elements, contains(
        new TableElement(Namespace.KEY, SchemaUtil.ROWKEY_NAME, new Type(SqlTypes.STRING)),
        new TableElement(Namespace.VALUE, BAR, new Type(SqlTypes.INTEGER))
    ));
  }

  @Test
  public void shouldParseQuotedSchema() {
    // Given:
    final String schema = "`END` VARCHAR";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(elements, hasItem(
        new TableElement(Namespace.VALUE, ColumnName.of("END"), new Type(SqlTypes.STRING))
    ));
  }

  @Test
  public void shouldParseQuotedMixedCase() {
    // Given:
    final String schema = "`End` VARCHAR";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(elements, hasItem(
        new TableElement(Namespace.VALUE, ColumnName.of("End"), new Type(SqlTypes.STRING))
    ));
  }

  @Test
  public void shouldParseEmptySchema() {
    // Given:
    final String schema = " \t\n\r";

    // When:
    final TableElements elements = parser.parse(schema);

    // Then:
    assertThat(Iterables.isEmpty(elements), is(true));
  }

  @Test
  public void shouldThrowOnInvalidSchema() {
    // Given:
    final String schema = "foo-bar INTEGER";

    // Expect:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Error parsing schema \"foo-bar INTEGER\" at 1:4: extraneous input '-' ");

    // When:
    parser.parse(schema);
  }

  @Test
  public void shouldThrowOnReservedWord() {
    // Given:
    final String schema = "CREATE INTEGER";

    // Expect:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Error parsing schema \"CREATE INTEGER\" at 1:1: extraneous input 'CREATE' ");

    // When:
    parser.parse(schema);
  }
}