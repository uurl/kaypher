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

package com.treutec.kaypher.query;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;

@Immutable
public class QueryId {

  private final String id;

  @JsonCreator
  public QueryId(final String id) {
    this.id = requireNonNull(id, "id");
  }

  @JsonValue
  public String getId() {
    return id;
  }

  public String toString() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryId)) {
      return false;
    }
    final QueryId queryId = (QueryId) o;
    return Objects.equals(id, queryId.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
