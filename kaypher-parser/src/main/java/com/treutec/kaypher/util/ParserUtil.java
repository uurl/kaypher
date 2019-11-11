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
package com.treutec.kaypher.util;

import static com.treutec.kaypher.parser.SqlBaseParser.DecimalLiteralContext;
import static java.util.Objects.requireNonNull;

import com.treutec.kaypher.execution.expression.tree.DoubleLiteral;
import com.treutec.kaypher.execution.expression.tree.IntegerLiteral;
import com.treutec.kaypher.execution.expression.tree.Literal;
import com.treutec.kaypher.execution.expression.tree.LongLiteral;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.NodeLocation;
import com.treutec.kaypher.parser.ParsingException;
import com.treutec.kaypher.parser.SqlBaseParser;
import com.treutec.kaypher.parser.SqlBaseParser.IntegerLiteralContext;
import com.treutec.kaypher.parser.SqlBaseParser.NumberContext;
import com.treutec.kaypher.parser.SqlBaseParser.SourceNameContext;
import com.treutec.kaypher.parser.exception.ParseFailedException;
import java.util.Optional;
import java.util.regex.Pattern;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

public final class ParserUtil {

  /**
   * Source names must adhere to the kafka topic naming convention. We restrict
   * it here instead of as a parser rule to allow for a more descriptive error
   * message and to avoid duplicated rules.
   *
   * @see org.apache.kafka.streams.state.StoreBuilder#name
   */
  private static final Pattern VALID_SOURCE_NAMES = Pattern.compile("[a-zA-Z0-9_-]*");

  private ParserUtil() {
  }

  public static SourceName getSourceName(final SourceNameContext sourceName) {
    final String text = getIdentifierText(sourceName.identifier());
    if (!VALID_SOURCE_NAMES.matcher(text).matches()) {
      throw new ParseFailedException(
          "Illegal argument at " + getLocation(sourceName).map(NodeLocation::toString).orElse("?")
              + ". Source names may only contain alphanumeric values, '_' or '-'. Got: '"
              + text + "'");
    }
    return SourceName.of(text);
  }

  public static String getIdentifierText(final SqlBaseParser.IdentifierContext context) {
    return getIdentifierText(false, context);
  }

  public static String getIdentifierText(
      final boolean caseSensitive,
      final SqlBaseParser.IdentifierContext context) {
    if (context instanceof SqlBaseParser.QuotedIdentifierAlternativeContext) {
      return unquote(context.getText(), "\"");
    } else if (context instanceof SqlBaseParser.BackQuotedIdentifierContext) {
      return unquote(context.getText(), "`");
    } else {
      return caseSensitive ? context.getText() : context.getText().toUpperCase();
    }
  }

  public static String unquote(final String value, final String quote) {
    return value.substring(1, value.length() - 1)
        .replace(quote + quote, quote);
  }

  public static int processIntegerNumber(final NumberContext number, final String context) {
    if (number instanceof SqlBaseParser.IntegerLiteralContext) {
      return ((IntegerLiteral) visitIntegerLiteral((IntegerLiteralContext) number)).getValue();
    }
    throw new KaypherException("Value must be integer for command: " + context);
  }

  public static Literal visitIntegerLiteral(final IntegerLiteralContext context) {
    final Optional<NodeLocation> location = getLocation(context);

    final long valueAsLong;
    try {
      valueAsLong = Long.parseLong(context.getText());
    } catch (final NumberFormatException e) {
      throw new ParsingException("Invalid numeric literal: " + context.getText(), location);
    }
    if (valueAsLong < 0) {
      throw new RuntimeException("Unexpected negative value in literal: " + valueAsLong);
    }

    if (valueAsLong <= Integer.MAX_VALUE) {
      return new IntegerLiteral(location, (int) valueAsLong);
    } else {
      return new LongLiteral(location, valueAsLong);
    }
  }

  public static DoubleLiteral parseDecimalLiteral(final DecimalLiteralContext context) {
    final Optional<NodeLocation> location = getLocation(context);

    try {
      final double value = Double.parseDouble(context.getText());
      if (Double.isNaN(value)) {
        throw new ParsingException("Not a number: " + context.getText(), location);
      }
      if (Double.isInfinite(value)) {
        throw new ParsingException("Number overflows DOUBLE: " + context.getText(), location);
      }
      return new DoubleLiteral(location, value);
    } catch (final NumberFormatException e) {
      throw new ParsingException("Invalid numeric literal: " + context.getText(), location);
    }
  }

  public static Optional<NodeLocation> getLocation(final TerminalNode terminalNode) {
    requireNonNull(terminalNode, "terminalNode is null");
    return getLocation(terminalNode.getSymbol());
  }

  public static Optional<NodeLocation> getLocation(final ParserRuleContext parserRuleContext) {
    requireNonNull(parserRuleContext, "parserRuleContext is null");
    return getLocation(parserRuleContext.getStart());
  }

  public static Optional<NodeLocation> getLocation(final Token token) {
    requireNonNull(token, "token is null");
    return Optional.of(new NodeLocation(token.getLine(), token.getCharPositionInLine()));
  }
}
