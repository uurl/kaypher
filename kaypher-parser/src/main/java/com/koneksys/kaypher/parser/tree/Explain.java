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

import static com.google.common.base.MoreObjects.toStringHelper;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class Explain extends Statement {

  private final Optional<Statement> statement;
  private final Optional<String> queryId;

  public Explain(
      final Optional<String> queryId,
      final Optional<Statement> statement
  ) {
    this(Optional.empty(), queryId, statement);
  }

  public Explain(
      final Optional<NodeLocation> location,
      final Optional<String> queryId,
      final Optional<Statement> statement
  ) {
    super(location);
    this.statement = Objects.requireNonNull(statement, "statement");
    this.queryId = queryId;

    if (statement.isPresent() == queryId.isPresent()) {
      throw new IllegalArgumentException("Must supply either queryId or statement");
    }
  }

  public Optional<Statement> getStatement() {
    return statement;
  }

  public Optional<String> getQueryId() {
    return queryId;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitExplain(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, queryId);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final Explain o = (Explain) obj;
    return Objects.equals(statement, o.statement)
           && Objects.equals(queryId, o.queryId);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("statement", statement)
        .add("queryId", queryId)
        .toString();
  }
}
