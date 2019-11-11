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

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.parser.NodeLocation;
import com.treutec.kaypher.parser.exception.ParseFailedException;
import com.treutec.kaypher.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class SingleColumn extends SelectItem {

  private final Optional<ColumnName> alias;
  private final Expression expression;

  public SingleColumn(
      final Expression expression,
      final Optional<ColumnName> alias
  ) {
    this(Optional.empty(), expression, alias);
  }

  public SingleColumn(
      final Optional<NodeLocation> location,
      final Expression expression,
      final Optional<ColumnName> alias
  ) {
    super(location);

    checkForReservedToken(expression, alias, SchemaUtil.ROWTIME_NAME);
    checkForReservedToken(expression, alias, SchemaUtil.ROWKEY_NAME);

    this.expression = requireNonNull(expression, "expression");
    this.alias = requireNonNull(alias, "alias");
  }

  public SingleColumn copyWithExpression(final Expression expression) {
    return new SingleColumn(getLocation(), expression, alias);
  }

  private static void checkForReservedToken(
      final Expression expression,
      final Optional<ColumnName> alias,
      final ColumnName reservedToken
  ) {
    if (!alias.isPresent()) {
      return;
    }
    if (alias.get().name().equalsIgnoreCase(reservedToken.name())) {
      final String text = expression.toString();
      if (!text.substring(text.indexOf(".") + 1).equalsIgnoreCase(reservedToken.name())) {
        throw new ParseFailedException(reservedToken.name()
            + " is a reserved token for implicit column. "
            + "You cannot use it as an alias for a column.");
      }
    }
  }

  public Optional<ColumnName> getAlias() {
    return alias;
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final SingleColumn other = (SingleColumn) obj;
    return Objects.equals(this.alias, other.alias)
        && Objects.equals(this.expression, other.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, expression);
  }

  @Override
  public String toString() {
    return "SingleColumn{"
        + ", alias=" + alias
        + ", expression=" + expression
        + '}';
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSingleColumn(this, context);
  }
}
