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

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import com.treutec.kaypher.execution.expression.tree.NullLiteral;
import com.treutec.kaypher.execution.expression.tree.StringLiteral;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.util.KaypherException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class InsertValuesTest {

  private static final SourceName SOME_NAME = SourceName.of("bob");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementEqualsHashcode() {
    new EqualsTester()
        .addEqualityGroup(
            new InsertValues(SOME_NAME, ImmutableList.of(), ImmutableList.of(new NullLiteral())),
            new InsertValues(SOME_NAME, ImmutableList.of(), ImmutableList.of(new NullLiteral())))
        .addEqualityGroup(new InsertValues(
            SourceName.of("diff"), ImmutableList.of(), ImmutableList.of(new StringLiteral("b"))))
        .addEqualityGroup(new InsertValues(
            SOME_NAME, ImmutableList.of(ColumnName.of("diff")), ImmutableList.of(new StringLiteral("b"))))
        .addEqualityGroup(new InsertValues(
            SOME_NAME, ImmutableList.of(), ImmutableList.of(new StringLiteral("diff"))))
        .testEquals();
  }

  @Test
  public void shouldThrowIfEmptyValues() {
    // Expect:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Expected some values for INSERT INTO statement");

    // When:
    new InsertValues(
        SOME_NAME,
        ImmutableList.of(ColumnName.of("col1")),
        ImmutableList.of());
  }

  @Test
  public void shouldThrowIfNonEmptyColumnsValuesDoNotMatch() {
    // Expect:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Expected number columns and values to match");

    // When:
    new InsertValues(
        SOME_NAME,
        ImmutableList.of(ColumnName.of("col1")),
        ImmutableList.of(new StringLiteral("val1"), new StringLiteral("val2")));
  }

}