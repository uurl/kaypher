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
import com.treutec.kaypher.execution.windows.KaypherWindowExpression;
import com.treutec.kaypher.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Immutable
public class WindowExpression extends AstNode {

  private final String windowName;
  private final KaypherWindowExpression kaypherWindowExpression;

  public WindowExpression(
      final String windowName,
      final KaypherWindowExpression kaypherWindowExpression
  ) {
    this(Optional.empty(), windowName, kaypherWindowExpression);
  }

  public WindowExpression(
      final Optional<NodeLocation> location,
      final String windowName,
      final KaypherWindowExpression kaypherWindowExpression
  ) {
    super(location);
    this.windowName = requireNonNull(windowName, "windowName");
    this.kaypherWindowExpression = requireNonNull(kaypherWindowExpression, "kaypherWindowExpression");
  }

  public KaypherWindowExpression getKaypherWindowExpression() {
    return kaypherWindowExpression;
  }

  public String getWindowName() {
    return windowName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WindowExpression that = (WindowExpression) o;
    return Objects.equals(windowName, that.windowName)
        && Objects.equals(kaypherWindowExpression, that.kaypherWindowExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(windowName, kaypherWindowExpression);
  }

  @Override
  public String toString() {
    return " WINDOW " + windowName + " " + kaypherWindowExpression;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitWindowExpression(this, context);
  }

  public static TimeUnit getWindowUnit(final String windowUnitString) {
    try {
      if (!windowUnitString.endsWith("S")) {
        return TimeUnit.valueOf(windowUnitString + "S");
      }
      return TimeUnit.valueOf(windowUnitString);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
