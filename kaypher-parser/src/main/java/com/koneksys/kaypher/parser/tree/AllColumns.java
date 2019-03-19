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
public class AllColumns extends SelectItem {

  private final Optional<QualifiedName> prefix;

  public AllColumns(final Optional<QualifiedName> prefix) {
    this(Optional.empty(), prefix);
  }

  public AllColumns(
      final Optional<NodeLocation> location,
      final Optional<QualifiedName> prefix
  ) {
    super(location);
    this.prefix = requireNonNull(prefix, "prefix");
  }

  public Optional<QualifiedName> getPrefix() {
    return prefix;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitAllColumns(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AllColumns that = (AllColumns) o;
    return Objects.equals(prefix, that.prefix);
  }

  @Override
  public int hashCode() {
    return prefix.hashCode();
  }

  @Override
  public String toString() {
    return prefix.map(qualifiedName -> qualifiedName + ".*").orElse("*");
  }
}