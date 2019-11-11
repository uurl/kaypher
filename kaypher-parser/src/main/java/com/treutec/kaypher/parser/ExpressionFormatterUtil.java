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

import static java.lang.String.format;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.execution.expression.formatter.ExpressionFormatter;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.parser.tree.GroupingElement;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.util.IdentifierUtil;
import java.util.List;
import java.util.Set;

public final class ExpressionFormatterUtil {
  private ExpressionFormatterUtil() {
  }

  public static String formatExpression(final Expression expression) {
    return formatExpression(expression, true);
  }

  public static String formatExpression(final Expression expression, final boolean unmangleNames) {
    return ExpressionFormatter.formatExpression(
        expression,
        unmangleNames,
        FormatOptions.of(IdentifierUtil::needsQuotes)
    );
  }

  public static String formatGroupBy(final List<GroupingElement> groupingElements) {
    final ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

    for (final GroupingElement groupingElement : groupingElements) {
      resultStrings.add(groupingElement.format());
    }
    return Joiner.on(", ").join(resultStrings.build());
  }

  public static String formatGroupingSet(final Set<Expression> groupingSet) {
    return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
            .map(ExpressionFormatterUtil::formatExpression)
            .iterator()));
  }

}
