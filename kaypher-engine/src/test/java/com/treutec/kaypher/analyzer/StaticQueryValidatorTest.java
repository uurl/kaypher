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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.treutec.kaypher.analyzer.Analysis.Into;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.parser.tree.ResultMaterialization;
import com.treutec.kaypher.parser.tree.WindowExpression;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.SchemaUtil;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StaticQueryValidatorTest {

  private static final Expression AN_EXPRESSION = mock(Expression.class);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Analysis analysis;
  @Mock
  private WindowExpression windowExpression;
  @Mock
  private Into into;

  private QueryValidator validator;

  @Before
  public void setUp() {
    validator = new StaticQueryValidator();

    when(analysis.getResultMaterialization()).thenReturn(ResultMaterialization.FINAL);
  }

  @Test
  public void shouldThrowOnStaticQueryThatIsNotFinal() {
    // Given:
    when(analysis.getResultMaterialization()).thenReturn(ResultMaterialization.CHANGES);

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Static queries don't support `EMIT CHANGES`");

    // When:
    validator.validate(analysis);
  }

  @Test(expected = KaypherException.class)
  public void shouldThrowOnStaticQueryIfSinkSupplied() {
    // Given:
    when(analysis.getInto()).thenReturn(Optional.of(into));

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnStaticQueryThatIsJoin() {
    // Given:
    when(analysis.isJoin()).thenReturn(true);

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Static queries don't support JOIN clauses.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnStaticQueryThatIsWindowed() {
    // Given:

    when(analysis.getWindowExpression()).thenReturn(Optional.of(windowExpression));

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Static queries don't support WINDOW clauses.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnGroupBy() {
    // Given:
    when(analysis.getGroupByExpressions()).thenReturn(ImmutableList.of(AN_EXPRESSION));

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Static queries don't support GROUP BY clauses.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnPartitionBy() {
    // Given:
    when(analysis.getPartitionBy()).thenReturn(Optional.of(ColumnRef.withoutSource(ColumnName.of("Something"))));

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Static queries don't support PARTITION BY clauses.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnHavingClause() {
    // Given:
    when(analysis.getHavingExpression()).thenReturn(Optional.of(AN_EXPRESSION));

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Static queries don't support HAVING clauses.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnLimitClause() {
    // Given:
    when(analysis.getLimitClause()).thenReturn(OptionalInt.of(1));

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Static queries don't support LIMIT clauses.");

    // When:
    validator.validate(analysis);
  }

  @Test
  public void shouldThrowOnRowTimeInProjection() {
    // Given:
    when(analysis.getSelectColumnRefs())
        .thenReturn(ImmutableSet.of(ColumnRef.withoutSource(SchemaUtil.ROWTIME_NAME)));

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Static queries don't support ROWTIME in select columns.");

    // When:
    validator.validate(analysis);
  }
}