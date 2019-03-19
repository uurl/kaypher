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
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Immutable
public abstract class GroupingElement extends Node {

  GroupingElement(final Optional<NodeLocation> location) {
    super(location);
  }

  public abstract List<Set<Expression>> enumerateGroupingSets();

  @Override
  protected <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitGroupingElement(this, context);
  }

  public abstract String format();
}
