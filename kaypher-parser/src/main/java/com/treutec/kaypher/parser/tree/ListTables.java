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

import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class ListTables extends Statement {

  private final boolean showExtended;

  public ListTables(final Optional<NodeLocation> location, final boolean showExtended) {
    super(location);
    this.showExtended = showExtended;
  }

  public boolean getShowExtended() {
    return showExtended;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitListTables(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ListTables that = (ListTables) o;
    return showExtended == that.showExtended;
  }

  @Override
  public int hashCode() {
    return Objects.hash(showExtended);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("showExtended", showExtended)
        .toString();
  }
}
