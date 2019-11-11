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
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.properties.with.CreateSourceAsProperties;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.util.KaypherException;
import java.util.Objects;
import java.util.Optional;

/**
 * Pojo holding sink information
 */
@Immutable
public final class Sink {

  private final SourceName name;
  private final boolean createSink;
  private final CreateSourceAsProperties properties;
  private final Optional<ColumnRef> partitionBy;

  /**
   * Info about the sink of a query.
   *
   * @param name the name of the sink.
   * @param createSink indicates if name should be created, (CSAS/CTAS), or not (INSERT INTO).
   * @param properties properties of the sink.
   * @param partitionBy an optional partition by expression
   * @return the pojo.
   */
  public static Sink of(
      final SourceName name,
      final boolean createSink,
      final CreateSourceAsProperties properties,
      final Optional<Expression> partitionBy
  ) {
    if (partitionBy.isPresent()) {
      final Expression partitionByExp = partitionBy.get();
      if (partitionByExp instanceof ColumnReferenceExp) {
        return new Sink(
            name,
            createSink,
            properties,
            Optional.of(((ColumnReferenceExp) partitionByExp).getReference())
        );
      }

      throw new KaypherException(
          "Expected partition by to be a valid column but got " + partitionByExp);
    }

    return new Sink(name, createSink, properties, Optional.empty());
  }

  private Sink(
      final SourceName name,
      final boolean createSink,
      final CreateSourceAsProperties properties,
      final Optional<ColumnRef> partitionBy
  ) {
    this.name = requireNonNull(name, "name");
    this.properties = requireNonNull(properties, "properties");
    this.createSink = createSink;
    this.partitionBy = Objects.requireNonNull(partitionBy, "partitionBy");
  }

  public SourceName getName() {
    return name;
  }

  public boolean shouldCreateSink() {
    return createSink;
  }

  public CreateSourceAsProperties getProperties() {
    return properties;
  }

  public Optional<ColumnRef> getPartitionBy() {
    return partitionBy;
  }
}
