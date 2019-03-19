/*
 * Copyright 2019 Koneksys
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

package com.koneksys.kaypher.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;

@Immutable
public final class Cast extends Expression {

  private final Expression expression;
  private final Type type;

  public Cast(
      final Expression expression,
      final Type type
  ) {
    this(Optional.empty(), expression, type);
  }

  public Cast(
      final Optional<NodeLocation> location,
      final Expression expression,
      final Type type
  ) {
    super(location);
    this.expression = requireNonNull(expression, "expression");
    this.type = requireNonNull(type, "type");
  }

  public Expression getExpression() {
    return expression;
  }

  public Type getType() {
    return type;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCast(this, context);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final Cast o = (Cast) obj;
    return Objects.equals(this.expression, o.expression)
           && Objects.equals(this.type, o.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, type);
  }
}