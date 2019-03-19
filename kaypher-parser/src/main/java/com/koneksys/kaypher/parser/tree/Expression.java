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

import com.google.errorprone.annotations.Immutable;
import com.koneksys.kaypher.parser.ExpressionFormatter;
import java.util.Optional;

/**
 * Expressions are used to declare select items, where and having clauses and join criteria in
 * queries.
 */
@Immutable
public abstract class Expression extends Node {

  protected Expression(final Optional<NodeLocation> location) {
    super(location);
  }

  /**
   * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
   */
  @Override
  protected <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitExpression(this, context);
  }

  @Override
  public final String toString() {
    return ExpressionFormatter.formatExpression(this);
  }

}
