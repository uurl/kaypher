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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.parser.tree.TableElement.Namespace;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.LogicalSchema.Builder;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.util.KaypherException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Immutable
public final class TableElements implements Iterable<TableElement> {

  private final ImmutableList<TableElement> elements;

  public static TableElements of(final TableElement... elements) {
    return new TableElements(ImmutableList.copyOf(elements));
  }

  public static TableElements of(final List<TableElement> elements) {
    return new TableElements(ImmutableList.copyOf(elements));
  }

  @Override
  public Iterator<TableElement> iterator() {
    return elements.iterator();
  }

  public Stream<TableElement> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableElements that = (TableElements) o;
    return Objects.equals(elements, that.elements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(elements);
  }

  @Override
  public String toString() {
    return elements.toString();
  }

  /**
   * @param withImplicitColumns controls if schema has implicit columns such as ROWTIME or ROWKEY.
   * @return the logical schema.
   */
  public LogicalSchema toLogicalSchema(final boolean withImplicitColumns) {
    if (Iterables.isEmpty(this)) {
      throw new KaypherException("No columns supplied.");
    }

    final Builder builder = withImplicitColumns
        ? LogicalSchema.builder()
        : LogicalSchema.builder().noImplicitColumns();

    for (final TableElement tableElement : this) {
      final ColumnName fieldName = tableElement.getName();
      final SqlType fieldType = tableElement.getType().getSqlType();

      if (tableElement.getNamespace() == Namespace.KEY) {
        builder.keyColumn(fieldName, fieldType);
      } else {
        builder.valueColumn(fieldName, fieldType);
      }
    }

    return builder.build();
  }

  private TableElements(final ImmutableList<TableElement> elements) {
    this.elements = Objects.requireNonNull(elements, "elements");

    throwOnDuplicateNames();
  }

  private void throwOnDuplicateNames() {
    final String duplicates = elements.stream()
        .collect(Collectors.groupingBy(TableElement::getName, Collectors.counting()))
        .entrySet()
        .stream()
        .filter(e -> e.getValue() > 1)
        .map(Entry::getKey)
        .map(ColumnName::toString)
        .collect(Collectors.joining(", "));

    if (!duplicates.isEmpty()) {
      throw new KaypherException("Duplicate column names: " + duplicates);
    }
  }
}
