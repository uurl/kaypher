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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.parser.KaypherParserTestUtil;
import com.treutec.kaypher.parser.tree.Query;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.util.MetaStoreFixture;
import com.treutec.kaypher.util.timestamp.PartialStringToTimestampParser;
import com.treutec.kaypher.util.timestamp.StringToTimestampParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class StatementRewriteForRowtimeTest {

  private static final StringToTimestampParser PARSER =
      new StringToTimestampParser("yyyy-MM-dd'T'HH:mm:ss.SSS");

  private static final long A_TIMESTAMP = 1234567890L;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private PartialStringToTimestampParser parser;
  private MetaStore metaStore;
  private StatementRewriteForRowtime rewritter;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
    rewritter = new StatementRewriteForRowtime(parser);

    when(parser.parse(any())).thenReturn(A_TIMESTAMP);
  }

  @Test
  public void shouldPassRowTimeStringsToTheParser() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where ROWTIME = '2017-01-01T00:44:00.000';");
    final Expression predicate = statement.getWhere().get();

    // When:
    rewritter.rewriteForRowtime(predicate);

    // Then:
    verify(parser).parse("2017-01-01T00:44:00.000");
  }

  @Test
  public void shouldReplaceDateString() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where ROWTIME > '2017-01-01T00:00:00.000';");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    assertThat(rewritten.toString(), is(String.format("(ORDERS.ROWTIME > %d)", A_TIMESTAMP)));
  }

  @Test
  public void shouldHandleBetweenExpression() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where ROWTIME BETWEEN '2017-01-01' AND '2017-02-01';");
    final Expression predicate = statement.getWhere().get();

    when(parser.parse(any()))
        .thenReturn(A_TIMESTAMP)
        .thenReturn(7654L);

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    assertThat(
        rewritten.toString(),
        is(String.format("(ORDERS.ROWTIME BETWEEN %d AND %d)", A_TIMESTAMP, 7654L))
    );
  }

  @Test
  public void shouldNotProcessStringsInFunctions() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where ROWTIME = foo('2017-01-01');");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    verify(parser, never()).parse(any());
    assertThat(rewritten.toString(), is("(ORDERS.ROWTIME = FOO('2017-01-01'))"));
  }

  @Test
  public void shouldIgnoreNonRowtimeStrings() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where ROWTIME > '2017-01-01' AND ROWKEY = '2017-01-01';");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    assertThat(rewritten.toString(),
        is(String.format("((ORDERS.ROWTIME > %d) AND (ORDERS.ROWKEY = '2017-01-01'))",
            A_TIMESTAMP)));
  }

  @Test
  public void shouldNotProcessWhenRowtimeInFunction() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where foo(ROWTIME) = '2017-01-01';");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    verify(parser, never()).parse(any());
    assertThat(rewritten.toString(), containsString("(FOO(ORDERS.ROWTIME) = '2017-01-01')"));
  }

  @Test
  public void shouldNotProcessArithmetic() {
    // Given:
    final Query statement = buildSingleAst(
        "SELECT * FROM orders where '2017-01-01' + 10000 > ROWTIME;");
    final Expression predicate = statement.getWhere().get();

    // When:
    final Expression rewritten = rewritter.rewriteForRowtime(predicate);

    // Then:
    verify(parser, never()).parse(any());
    assertThat(rewritten.toString(), containsString("(('2017-01-01' + 10000) > ORDERS.ROWTIME)"));
  }

  @Test
  public void shouldThrowParseError() {
    // Given:
    final Query statement = buildSingleAst("SELECT * FROM orders where ROWTIME = '2017-01-01';");
    final Expression predicate = statement.getWhere().get();
    when(parser.parse(any())).thenThrow(new IllegalArgumentException("it no good"));

    // Expect:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("it no good");

    // When:
    rewritter.rewriteForRowtime(predicate);
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> T buildSingleAst(final String query) {
    return (T) KaypherParserTestUtil.buildSingleAst(query, metaStore).getStatement();
  }
}