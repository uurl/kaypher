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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.parser.ExpressionFormatterUtil;
import com.treutec.kaypher.parser.NodeLocation;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Immutable
public class SimpleGroupBy extends GroupingElement {

  private final ImmutableList<Expression> columns;

  public SimpleGroupBy(final List<Expression> columns) {
    this(Optional.empty(), columns);
  }

  public SimpleGroupBy(
      final Optional<NodeLocation> location,
      final List<Expression> columns
  ) {
    super(location);
    this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns"));
  }

  public List<Expression> getColumns() {
    return columns;
  }

  @Override
  public List<Set<Expression>> enumerateGroupingSets() {
    return ImmutableList.of(ImmutableSet.copyOf(columns));
  }

  @Override
  protected <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSimpleGroupBy(this, context);
  }

  @Override
  public String format() {
    final Set<Expression>
        columns =
        ImmutableSet.copyOf(this.columns);
    if (columns.size() == 1) {
      return ExpressionFormatterUtil.formatExpression(getOnlyElement(columns));
    }
    return ExpressionFormatterUtil.formatGroupingSet(columns);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SimpleGroupBy that = (SimpleGroupBy) o;
    return Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("columns", columns)
        .toString();
  }
}
