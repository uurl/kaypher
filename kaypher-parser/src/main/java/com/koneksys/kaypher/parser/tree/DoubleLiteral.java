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
import java.util.Optional;

@Immutable
public class DoubleLiteral extends Literal {

  private final double value;

  public DoubleLiteral(final String value) {
    this(Optional.empty(), value);
  }

  public DoubleLiteral(final Optional<NodeLocation> location, final String value) {
    super(location);
    this.value = Double.parseDouble(requireNonNull(value, "value"));
  }

  @Override
  public Double getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDoubleLiteral(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DoubleLiteral that = (DoubleLiteral) o;

    if (Double.compare(that.value, value) != 0) {
      return false;
    }

    return true;
  }

  @SuppressWarnings("UnaryPlus")
  @Override
  public int hashCode() {
    final long temp = value != +0.0d ? Double.doubleToLongBits(value) : 0L;
    return (int) (temp ^ (temp >>> 32));
  }
}
