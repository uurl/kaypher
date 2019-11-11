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
package com.treutec.kaypher.parser.util;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.treutec.kaypher.parser.ParsingException;
import com.treutec.kaypher.parser.SqlBaseParser.DecimalLiteralContext;
import com.treutec.kaypher.util.ParserUtil;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ParserUtilTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private DecimalLiteralContext decimalLiteralContext;

  @Before
  public void setUp() {
    mockLocation(decimalLiteralContext, 1, 2);
  }

  @Test
  public void shouldThrowWhenParsingDecimalIfNaN() {
    // Given:
    when(decimalLiteralContext.getText()).thenReturn("NaN");

    // Then:
    expectedException.expect(ParsingException.class);
    expectedException.expectMessage("line 1:4: Not a number: NaN");

    // When:
    ParserUtil.parseDecimalLiteral(decimalLiteralContext);
  }

  @Test
  public void shouldThrowWhenParsingDecimalIfNotDecimal() {
    // Given:
    when(decimalLiteralContext.getText()).thenReturn("What?");

    // Then:
    expectedException.expect(ParsingException.class);
    expectedException.expectMessage("line 1:4: Invalid numeric literal: What?");

    // When:
    ParserUtil.parseDecimalLiteral(decimalLiteralContext);
  }

  @Test
  public void shouldThrowWhenParsingDecimalIfOverflowsDouble() {
    // Given:
    when(decimalLiteralContext.getText()).thenReturn("1.7976931348623159E308");

    // Then:
    expectedException.expect(ParsingException.class);
    expectedException.expectMessage("line 1:4: Number overflows DOUBLE: 1.7976931348623159E308");

    // When:
    ParserUtil.parseDecimalLiteral(decimalLiteralContext);
  }

  private static void mockLocation(final ParserRuleContext ctx, final int line, final int col) {
    final Token token = mock(Token.class);
    when(token.getLine()).thenReturn(line);
    when(token.getCharPositionInLine()).thenReturn(col);
    when(ctx.getStart()).thenReturn(token);
  }
}
