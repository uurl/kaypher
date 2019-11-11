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
package com.treutec.kaypher.parser.tree;

import static com.treutec.kaypher.parser.tree.TableElement.Namespace.KEY;
import static com.treutec.kaypher.parser.tree.TableElement.Namespace.VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.treutec.kaypher.execution.expression.tree.Type;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.parser.NodeLocation;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.util.SchemaUtil;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TableElementTest {

  private static final Optional<NodeLocation> A_LOCATION =
      Optional.of(new NodeLocation(2, 4));
  
  private static final ColumnName NAME = ColumnName.of("name");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            new TableElement(A_LOCATION, VALUE, NAME, new Type(SqlTypes.STRING)),
            new TableElement(VALUE, NAME, new Type(SqlTypes.STRING))
        )
        .addEqualityGroup(
            new TableElement(VALUE, ColumnName.of("different"), new Type(SqlTypes.STRING))
        )
        .addEqualityGroup(
            new TableElement(VALUE, NAME, new Type(SqlTypes.INTEGER))
        )
        .addEqualityGroup(
            new TableElement(KEY, SchemaUtil.ROWKEY_NAME, new Type(SqlTypes.STRING))
        )
        .testEquals();
  }

  @Test
  public void shouldReturnName() {
    // Given:
    final TableElement element =
        new TableElement(VALUE, NAME, new Type(SqlTypes.STRING));

    // Then:
    assertThat(element.getName(), is(NAME));
  }

  @Test
  public void shouldReturnType() {
    // Given:
    final TableElement element = new TableElement(VALUE, NAME, new Type(SqlTypes.STRING));

    // Then:
    assertThat(element.getType(), is(new Type(SqlTypes.STRING)));
  }

  @Test
  public void shouldReturnNamespace() {
    // Given:
    final TableElement valueElement = new TableElement(VALUE, NAME, new Type(SqlTypes.STRING));

    // Then:
    assertThat(valueElement.getNamespace(), is(VALUE));
  }
}