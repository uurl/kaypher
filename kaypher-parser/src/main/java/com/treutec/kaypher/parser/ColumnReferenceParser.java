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

import static com.treutec.kaypher.util.ParserUtil.getLocation;

import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.SqlBaseParser.ColumnReferenceContext;
import com.treutec.kaypher.parser.SqlBaseParser.PrimaryExpressionContext;
import com.treutec.kaypher.parser.exception.ParseFailedException;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.util.ParserUtil;
import java.util.Optional;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public final class ColumnReferenceParser {

  private ColumnReferenceParser() {
  }

  /**
   * Parses text that represents a column (such as values referencing keys/timestamps
   * in the WITH clause). This will use the same semantics as the {@link AstBuilder},
   * namely it will uppercase anything that is unquoted, and remove quotes but leave
   * the case sensitivity for anything that is quoted. If the text contains an unquoted
   * ".", it is considered a delimiter between the source and the column name.
   *
   * @param text the text to parse
   * @return a {@code ColumnRef}
   * @throws ParseFailedException if the parse fails (this does not match the pattern
   *                              for a column reference
   */
  public static ColumnRef parse(final String text) {
    final SqlBaseLexer lexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString(text))
    );
    final CommonTokenStream tokStream = new CommonTokenStream(lexer);
    final SqlBaseParser parser = new SqlBaseParser(tokStream);

    final PrimaryExpressionContext primaryExpression = parser.primaryExpression();
    if (primaryExpression instanceof ColumnReferenceContext) {
      return resolve((ColumnReferenceContext) primaryExpression).getReference();
    }

    throw new ParseFailedException("Cannot parse text that is not column reference: " + text);
  }

  static ColumnReferenceExp resolve(final ColumnReferenceContext context) {
    final ColumnName columnName;
    final SourceName sourceName;

    if (context.identifier(1) == null) {
      sourceName = null;
      columnName = ColumnName.of(ParserUtil.getIdentifierText(context.identifier(0)));
    } else {
      sourceName = SourceName.of(ParserUtil.getIdentifierText(context.identifier(0)));
      columnName = ColumnName.of(ParserUtil.getIdentifierText(context.identifier(1)));
    }

    return new ColumnReferenceExp(
        getLocation(context),
        ColumnRef.of(Optional.ofNullable(sourceName), columnName)
    );
  }

}
