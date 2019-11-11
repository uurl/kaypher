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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.NodeLocation;
import com.treutec.kaypher.util.KaypherException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Immutable
public class InsertValues extends Statement {

  private final SourceName target;
  private final ImmutableList<ColumnName> columns;
  private final ImmutableList<Expression> values;

  public InsertValues(
      final SourceName target,
      final List<ColumnName> columns,
      final List<Expression> values
  ) {
    this(Optional.empty(), target, columns, values);
  }

  public InsertValues(
      final Optional<NodeLocation> location,
      final SourceName target,
      final List<ColumnName> columns,
      final List<Expression> values
  ) {
    super(location);
    this.target = Objects.requireNonNull(target, "target");
    this.columns = ImmutableList.copyOf(Objects.requireNonNull(columns, "columns"));
    this.values = ImmutableList.copyOf(Objects.requireNonNull(values, "values"));

    if (values.isEmpty()) {
      throw new KaypherException("Expected some values for INSERT INTO statement.");
    }

    if (!columns.isEmpty() && columns.size() != values.size()) {
      throw new KaypherException(
          "Expected number columns and values to match: "
              + columns.stream().map(ColumnName::name).collect(Collectors.toList()) + ", "
              + values);
    }
  }

  public SourceName getTarget() {
    return target;
  }

  public List<ColumnName> getColumns() {
    return columns;
  }

  public List<Expression> getValues() {
    return values;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitInsertValues(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final InsertValues that = (InsertValues) o;
    return Objects.equals(target, that.target)
        && Objects.equals(columns, that.columns)
        && Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(target, columns, values);
  }

  @Override
  public String toString() {
    return "InsertValues{"
        + "target=" + target
        + ", columns=" + columns
        + ", values=" + values
        + '}';
  }
}
