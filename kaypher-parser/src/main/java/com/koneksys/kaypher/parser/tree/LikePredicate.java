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
public class LikePredicate extends Expression {

  private final Expression value;
  private final Expression pattern;

  public LikePredicate(
      final Expression value,
      final Expression pattern
  ) {
    this(Optional.empty(), value, pattern);
  }

  public LikePredicate(
      final Optional<NodeLocation> location,
      final Expression value,
      final Expression pattern
  ) {
    super(location);
    this.value = requireNonNull(value, "value");
    this.pattern = requireNonNull(pattern, "pattern");
  }

  public Expression getValue() {
    return value;
  }

  public Expression getPattern() {
    return pattern;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitLikePredicate(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final LikePredicate that = (LikePredicate) o;
    return Objects.equals(value, that.value)
           && Objects.equals(pattern, that.pattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, pattern);
  }
}
