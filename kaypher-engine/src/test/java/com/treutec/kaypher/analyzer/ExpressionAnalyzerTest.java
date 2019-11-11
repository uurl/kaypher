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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.ComparisonExpression;
import com.treutec.kaypher.execution.expression.tree.ComparisonExpression.Type;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.StringLiteral;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.SchemaUtil;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExpressionAnalyzerTest {

  private static final Expression WINDOW_START_EXP = new ColumnReferenceExp(
      ColumnRef.of(SourceName.of("something"), SchemaUtil.WINDOWSTART_NAME)
  );

  private static final Expression OTHER_EXP = new StringLiteral("foo");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SourceSchemas sourceSchemas;
  private ExpressionAnalyzer analyzer;

  @Before
  public void setUp() {
    analyzer = new ExpressionAnalyzer(sourceSchemas);
  }

  @Test
  public void shouldNotThrowOnWindowStartIfAllowed() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        WINDOW_START_EXP,
        OTHER_EXP
    );

    // When:
    analyzer.analyzeExpression(expression, true);

    // Then: did not throw
  }

  @Test
  public void shouldThrowOnWindowStartIfNotAllowed() {
    // Given:
    final Expression expression = new ComparisonExpression(
        Type.EQUAL,
        WINDOW_START_EXP,
        OTHER_EXP
    );

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(
        "Column 'something.WINDOWSTART' cannot be resolved.");

    // When:
    analyzer.analyzeExpression(expression, false);
  }

  @Test
  public void shouldGetSourcesUsingFullColumnRef() {
    // Given:
    final ColumnRef column = ColumnRef.of(SourceName.of("fully"), ColumnName.of("qualified"));
    final Expression expression = new ColumnReferenceExp(column);

    when(sourceSchemas.sourcesWithField(any())).thenReturn(sourceNames("something"));

    // When:
    analyzer.analyzeExpression(expression, true);

    // Then:
    verify(sourceSchemas).sourcesWithField(column);
  }

  @Test
  public void shouldThrowOnMultipleSources() {
    // Given:
    final Expression expression = new ColumnReferenceExp(
        ColumnRef.withoutSource(ColumnName.of("just-name"))
    );

    when(sourceSchemas.sourcesWithField(any()))
        .thenReturn(sourceNames("multiple", "sources"));

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(
        "Column 'just-name' is ambiguous. Could be any of: multiple.just-name, sources.just-name");

    // When:
    analyzer.analyzeExpression(expression, true);
  }

  @Test
  public void shouldThrowOnNoSources() {
    // Given:
    final Expression expression = new ColumnReferenceExp(
        ColumnRef.withoutSource(ColumnName.of("just-name"))
    );

    when(sourceSchemas.sourcesWithField(any()))
        .thenReturn(ImmutableSet.of());

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(
        "Column 'just-name' cannot be resolved.");

    // When:
    analyzer.analyzeExpression(expression, true);
  }

  private static Set<SourceName> sourceNames(final String... names) {
    return Arrays.stream(names)
        .map(SourceName::of)
        .collect(Collectors.toSet());
  }
}