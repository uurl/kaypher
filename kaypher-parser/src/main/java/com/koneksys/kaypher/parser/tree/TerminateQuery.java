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
import com.koneksys.kaypher.query.QueryId;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class TerminateQuery extends Statement {

  private final QueryId queryId;

  public TerminateQuery(final String queryId) {
    this(Optional.empty(), queryId);
  }

  public TerminateQuery(final Optional<NodeLocation> location, final String queryId) {
    super(location);
    this.queryId = new QueryId(queryId);
  }

  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TerminateQuery that = (TerminateQuery) o;
    return Objects.equals(queryId, that.queryId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(queryId);
  }

  @Override
  public String toString() {
    return "TerminateQuery{"
        + "queryId=" + queryId
        + '}';
  }
}
