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
package com.treutec.kaypher.engine.rewrite;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.engine.rewrite.ExpressionTreeRewriter.Context;
import com.treutec.kaypher.execution.expression.tree.ArithmeticBinaryExpression;
import com.treutec.kaypher.execution.expression.tree.ArithmeticUnaryExpression;
import com.treutec.kaypher.execution.expression.tree.BetweenPredicate;
import com.treutec.kaypher.execution.expression.tree.BooleanLiteral;
import com.treutec.kaypher.execution.expression.tree.Cast;
import com.treutec.kaypher.execution.expression.tree.ComparisonExpression;
import com.treutec.kaypher.execution.expression.tree.DecimalLiteral;
import com.treutec.kaypher.execution.expression.tree.DereferenceExpression;
import com.treutec.kaypher.execution.expression.tree.DoubleLiteral;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.expression.tree.InListExpression;
import com.treutec.kaypher.execution.expression.tree.InPredicate;
import com.treutec.kaypher.execution.expression.tree.IntegerLiteral;
import com.treutec.kaypher.execution.expression.tree.IsNotNullPredicate;
import com.treutec.kaypher.execution.expression.tree.IsNullPredicate;
import com.treutec.kaypher.execution.expression.tree.LikePredicate;
import com.treutec.kaypher.execution.expression.tree.LogicalBinaryExpression;
import com.treutec.kaypher.execution.expression.tree.LongLiteral;
import com.treutec.kaypher.execution.expression.tree.NotExpression;
import com.treutec.kaypher.execution.expression.tree.NullLiteral;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.SearchedCaseExpression;
import com.treutec.kaypher.execution.expression.tree.SimpleCaseExpression;
import com.treutec.kaypher.execution.expression.tree.StringLiteral;
import com.treutec.kaypher.execution.expression.tree.SubscriptExpression;
import com.treutec.kaypher.execution.expression.tree.TimeLiteral;
import com.treutec.kaypher.execution.expression.tree.TimestampLiteral;
import com.treutec.kaypher.execution.expression.tree.Type;
import com.treutec.kaypher.execution.expression.tree.WhenClause;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.KaypherParserTestUtil;
import com.treutec.kaypher.parser.tree.Query;
import com.treutec.kaypher.parser.tree.SelectItem;
import com.treutec.kaypher.parser.tree.SingleColumn;
import com.treutec.kaypher.schema.kaypher.types.SqlPrimitiveType;
import com.treutec.kaypher.util.MetaStoreFixture;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExpressionTreeRewriterTest {

  private static final List<Expression> LITERALS = ImmutableList.of(
      new IntegerLiteral(1),
      new LongLiteral(1),
      new DoubleLiteral(1.0),
      new BooleanLiteral("true"),
      new StringLiteral("abcd"),
      new NullLiteral(),
      new DecimalLiteral("1.0"),
      new TimeLiteral("00:00:00"),
      new TimestampLiteral("00:00:00")
  );

  private MetaStore metaStore;

  @Mock
  private BiFunction<Expression, Context<Object>, Optional<Expression>> plugin;
  @Mock
  private BiFunction<Expression, Object, Expression> processor;
  @Mock
  private Expression expr1;
  @Mock
  private Expression expr2;
  @Mock
  private Expression expr3;
  @Mock
  private InListExpression inList;
  @Mock
  private WhenClause when1;
  @Mock
  private WhenClause when2;
  @Mock
  private Type type;
  @Mock
  private Object context;

  private ExpressionTreeRewriter<Object> expressionRewriter;
  private ExpressionTreeRewriter<Object> expressionRewriterWithPlugin;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
    expressionRewriter = new ExpressionTreeRewriter<>((e, c) -> Optional.empty(), processor);
    expressionRewriterWithPlugin = new ExpressionTreeRewriter<>(plugin, processor);
  }

  @SuppressWarnings("unchecked")
  private void shouldRewriteUsingPlugin(final Expression parsed) {
    // Given:
    when(plugin.apply(any(), any())).thenReturn(Optional.of(expr1));

    // When:
    final Expression rewritten = expressionRewriterWithPlugin.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, is(expr1));
    final ArgumentCaptor<Context> captor = ArgumentCaptor.forClass(Context.class);
    verify(plugin).apply(same(parsed), captor.capture());
    assertThat(captor.getValue().getContext(), is(context));
  }

  @Test
  public void shouldRewriteArithmeticBinary() {
    // Given:
    final ArithmeticBinaryExpression parsed = parseExpression("1 + 2");
    when(processor.apply(parsed.getLeft(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getRight(), context)).thenReturn(expr2);

    // When
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new ArithmeticBinaryExpression(
                parsed.getLocation(),
                parsed.getOperator(),
                expr1,
                expr2
            )
        )
    );
  }

  @Test
  public void shouldRewriteArithmeticBinaryUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("1 + 2");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteBetweenPredicate() {
    // Given:
    final BetweenPredicate parsed = parseExpression("1 BETWEEN 0 AND 2");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getMin(), context)).thenReturn(expr2);
    when(processor.apply(parsed.getMax(), context)).thenReturn(expr3);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new BetweenPredicate(parsed.getLocation(), expr1, expr2, expr3))
    );
  }

  @Test
  public void shouldRewriteBetweenPredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("1 BETWEEN 0 AND 2");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteComparisonExpression() {
    // Given:
    final ComparisonExpression parsed = parseExpression("1 < 2");
    when(processor.apply(parsed.getLeft(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getRight(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new ComparisonExpression(parsed.getLocation(), parsed.getType(), expr1, expr2))
    );
  }

  @Test
  public void shouldRewriteComparisonExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("1 < 2");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteInListExpression() {
    // Given:
    final InPredicate inPredicate = parseExpression("1 IN (1, 2, 3)");
    final InListExpression parsed = inPredicate.getValueList();
    when(processor.apply(parsed.getValues().get(0), context)).thenReturn(expr1);
    when(processor.apply(parsed.getValues().get(1), context)).thenReturn(expr2);
    when(processor.apply(parsed.getValues().get(2), context)).thenReturn(expr3);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new InListExpression(parsed.getLocation(), ImmutableList.of(expr1, expr2, expr3)))
    );
  }

  @Test
  public void shouldRewriteInListUsingPlugin() {
    // Given:
    final InPredicate inPredicate = parseExpression("1 IN (1, 2, 3)");
    final InListExpression parsed = inPredicate.getValueList();

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteFunctionCall() {
    // Given:
    final FunctionCall parsed = parseExpression("STRLEN('foo')");
    when(processor.apply(parsed.getArguments().get(0), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new FunctionCall(parsed.getLocation(), parsed.getName(), ImmutableList.of(expr1)))
    );
  }

  @Test
  public void shouldRewriteFunctionCallUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("STRLEN('foo')");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteSimpleCaseExpression() {
    // Given:
    final SimpleCaseExpression parsed = parseExpression(
        "CASE COL0 WHEN 1 THEN 'ONE' WHEN 2 THEN 'TWO' ELSE 'THREE' END");
    when(processor.apply(parsed.getOperand(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getWhenClauses().get(0), context)).thenReturn(when1);
    when(processor.apply(parsed.getWhenClauses().get(1), context)).thenReturn(when2);
    when(processor.apply(parsed.getDefaultValue().get(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new SimpleCaseExpression(
                parsed.getLocation(),
                expr1,
                ImmutableList.of(when1, when2),
                Optional.of(expr2)
            )
        )
    );
  }

  @Test
  public void shouldRewriteSimpleCaseExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression(
        "CASE COL0 WHEN 1 THEN 'ONE' WHEN 2 THEN 'TWO' ELSE 'THREE' END"
    );

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteInPredicate() {
    // Given:
    final InPredicate parsed = parseExpression("1 IN (1, 2, 3)");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getValueList(), context)).thenReturn(inList);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new InPredicate(parsed.getLocation(), expr1, inList)));
  }

  @Test
  public void shouldRewriteInPredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("1 IN (1, 2, 3)");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteDereferenceExpression() {
    // Given:
    final DereferenceExpression parsed = parseExpression("col0->foo");
    when(processor.apply(parsed.getBase(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new DereferenceExpression(parsed.getLocation(), expr1, parsed.getFieldName()))
    );
  }

  @Test
  public void shouldRewriteDereferenceExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col0->foo");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteArithmeticUnary() {
    // Given:
    final ArithmeticUnaryExpression parsed = parseExpression("-1");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(new ArithmeticUnaryExpression(parsed.getLocation(), parsed.getSign(), expr1))
    );
  }

  @Test
  public void shouldRewriteArithmeticUnaryUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("-1");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteNotExpression() {
    // Given:
    final NotExpression parsed = parseExpression("NOT 1 < 2");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new NotExpression(parsed.getLocation(), expr1)));
  }

  @Test
  public void shouldRewriteNotExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("NOT 1 < 2");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteSearchedCaseExpression() {
    // Given:
    final SearchedCaseExpression parsed = parseExpression(
        "CASE WHEN col0=1 THEN 'one' WHEN col0=2 THEN 'two' ELSE 'three' END");
    when(processor.apply(parsed.getWhenClauses().get(0), context)).thenReturn(when1);
    when(processor.apply(parsed.getWhenClauses().get(1), context)).thenReturn(when2);
    when(processor.apply(any(StringLiteral.class), any())).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new SearchedCaseExpression(
                parsed.getLocation(),
                ImmutableList.of(when1, when2),
                Optional.of(expr1)
            )
        )
    );
  }

  @Test
  public void shouldRewriteSearchedCaseExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression(
        "CASE WHEN col0=1 THEN 'one' WHEN col0=2 THEN 'two' ELSE 'three' END");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteLikePredicate() {
    // Given:
    final LikePredicate parsed = parseExpression("col1 LIKE '%foo%'");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getPattern(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new LikePredicate(parsed.getLocation(), expr1, expr2)));
  }

  @Test
  public void shouldRewriteLikePredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col1 LIKE '%foo%'");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteIsNotNullPredicate() {
    // Given:
    final IsNotNullPredicate parsed = parseExpression("col0 IS NOT NULL");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new IsNotNullPredicate(parsed.getLocation(), expr1)));
  }

  @Test
  public void shouldRewriteIsNotNullPredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col0 IS NOT NULL");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteIsNullPredicate() {
    // Given:
    final IsNullPredicate parsed = parseExpression("col0 IS NULL");
    when(processor.apply(parsed.getValue(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new IsNullPredicate(parsed.getLocation(), expr1)));
  }

  @Test
  public void shouldRewriteIsNullPredicateUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col0 IS NULL");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteSubscriptExpression() {
    // Given:
    final SubscriptExpression parsed = parseExpression("col4[1]");
    when(processor.apply(parsed.getBase(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getIndex(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new SubscriptExpression(parsed.getLocation(), expr1, expr2)));
  }

  @Test
  public void shouldRewriteSubscriptExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("col4[1]");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteLogicalBinaryExpression() {
    // Given:
    final LogicalBinaryExpression parsed = parseExpression("true OR false");
    when(processor.apply(parsed.getLeft(), context)).thenReturn(expr1);
    when(processor.apply(parsed.getRight(), context)).thenReturn(expr2);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(
        rewritten,
        equalTo(
            new LogicalBinaryExpression(parsed.getLocation(), parsed.getType(), expr1, expr2))
    );
  }

  @Test
  public void shouldRewriteLogicalBinaryExpressionUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("true OR false");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteCast() {
    // Given:
    final Cast parsed = parseExpression("CAST(col0 AS INTEGER)");
    when(processor.apply(parsed.getType(), context)).thenReturn(type);
    when(processor.apply(parsed.getExpression(), context)).thenReturn(expr1);

    // When:
    final Expression rewritten = expressionRewriter.rewrite(parsed, context);

    // Then:
    assertThat(rewritten, equalTo(new Cast(parsed.getLocation(), expr1, type)));
  }

  @Test
  public void shouldRewriteCastUsingPlugin() {
    // Given:
    final Expression parsed = parseExpression("CAST(col0 AS INTEGER)");

    // When/Then:
    shouldRewriteUsingPlugin(parsed);
  }

  @Test
  public void shouldRewriteQualifiedNameReference() {
    // Given:
    final ColumnReferenceExp expression = new ColumnReferenceExp(ColumnRef.withoutSource(
        ColumnName.of("foo")));

    // When:
    final Expression rewritten = expressionRewriter.rewrite(expression, context);

    // Then:
    assertThat(rewritten, is(expression));
  }

  @Test
  public void shouldRewriteQualifiedNameReferenceUsingPlugin() {
    // Given:
    final ColumnReferenceExp expression = new ColumnReferenceExp(ColumnRef.withoutSource(
        ColumnName.of("foo")));

    // When/Then:
    shouldRewriteUsingPlugin(expression);
  }

  @Test
  public void shouldRewriteLiteral() {
    for (final Expression expression : LITERALS) {
      // When:
      final Expression rewritten = expressionRewriter.rewrite(expression, context);

      // Then:
      assertThat(rewritten, is(expression));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRewriteLiteralUsingPlugin() {
    for (final Expression expression : LITERALS) {
      reset(plugin);
      shouldRewriteUsingPlugin(expression);
    }
  }

  @Test
  public void shouldRewriteType() {
    // Given:
    final Type type = new Type(SqlPrimitiveType.of("INTEGER"));

    // When:
    final Expression rewritten = expressionRewriter.rewrite(type, context);

    // Then:
    assertThat(rewritten, is(type));
  }

  @Test
  public void shouldRewriteTypeUsingPlugin() {
    final Type type = new Type(SqlPrimitiveType.of("INTEGER"));
    shouldRewriteUsingPlugin(type);
  }

  @SuppressWarnings("unchecked")
  private <T extends Expression> T parseExpression(final String asText) {
    final String kaypher = String.format("SELECT %s FROM test1;", asText);

    final PreparedStatement<Query> stmt = KaypherParserTestUtil.buildSingleAst(kaypher, metaStore);
    final SelectItem selectItem = stmt.getStatement().getSelect().getSelectItems().get(0);
    return (T) ((SingleColumn) selectItem).getExpression();
  }
}
