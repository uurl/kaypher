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

import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.NodeLocation;
import com.treutec.kaypher.parser.properties.with.CreateSourceAsProperties;
import java.util.Objects;
import java.util.Optional;

public abstract class CreateAsSelect extends Statement implements QueryContainer {

  private final SourceName name;
  private final Query query;
  private final boolean notExists;
  private final CreateSourceAsProperties properties;
  private final Optional<Expression> partitionByColumn;

  CreateAsSelect(
      final Optional<NodeLocation> location,
      final SourceName name,
      final Query query,
      final boolean notExists,
      final CreateSourceAsProperties properties,
      final Optional<Expression> partitionByColumn
  ) {
    super(location);
    this.name = requireNonNull(name, "name");
    this.query = requireNonNull(query, "query");
    this.notExists = notExists;
    this.properties = requireNonNull(properties, "properties");
    this.partitionByColumn = requireNonNull(partitionByColumn, "partitionByColumn");
  }

  CreateAsSelect(
      final CreateAsSelect other,
      final CreateSourceAsProperties properties
  ) {
    this(
        other.getLocation(),
        other.name,
        other.query,
        other.notExists,
        properties,
        other.partitionByColumn);
  }

  public abstract CreateAsSelect copyWith(CreateSourceAsProperties properties);

  public SourceName getName() {
    return name;
  }

  @Override
  public Query getQuery() {
    return query;
  }

  public boolean isNotExists() {
    return notExists;
  }

  public CreateSourceAsProperties getProperties() {
    return properties;
  }

  public Optional<Expression> getPartitionByColumn() {
    return partitionByColumn;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, query, properties, notExists, getClass());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final CreateAsSelect o = (CreateAsSelect) obj;
    return Objects.equals(name, o.name)
        && Objects.equals(query, o.query)
        && Objects.equals(notExists, o.notExists)
        && Objects.equals(properties, o.properties)
        && Objects.equals(partitionByColumn, o.partitionByColumn);
  }

  @Override
  public String toString() {
    return "CreateAsSelect{" + "name=" + name
        + ", query=" + query
        + ", notExists=" + notExists
        + ", properties=" + properties
        + ", partitionByColumn=" + partitionByColumn
        + '}';
  }
}
