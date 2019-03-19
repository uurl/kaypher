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

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import com.koneksys.kaypher.GenericRow;
import com.koneksys.kaypher.function.UdafAggregator;
import com.koneksys.kaypher.metastore.SerdeFactory;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

@Immutable
public class SessionWindowExpression extends KaypherWindowExpression {

  private final long gap;
  private final TimeUnit sizeUnit;

  public SessionWindowExpression(final long gap, final TimeUnit sizeUnit) {
    this(Optional.empty(), gap, sizeUnit);
  }

  public SessionWindowExpression(
      final Optional<NodeLocation> location,
      final long gap,
      final TimeUnit sizeUnit
  ) {
    super(location);
    this.gap = gap;
    this.sizeUnit = requireNonNull(sizeUnit, "sizeUnit");
  }

  public long getGap() {
    return gap;
  }

  public TimeUnit getSizeUnit() {
    return sizeUnit;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSessionWindowExpression(this, context);
  }

  @Override
  public String toString() {
    return " SESSION ( " + gap + " " + sizeUnit + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(gap, sizeUnit);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SessionWindowExpression sessionWindowExpression = (SessionWindowExpression) o;
    return sessionWindowExpression.gap == gap && sessionWindowExpression.sizeUnit == sizeUnit;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KTable applyAggregate(final KGroupedStream groupedStream,
                               final Initializer initializer,
                               final UdafAggregator aggregator,
                               final Materialized<String, GenericRow, ?> materialized) {

    final SessionWindows windows = SessionWindows.with(Duration.ofMillis(sizeUnit.toMillis(gap)));

    return groupedStream
        .windowedBy(windows)
        .aggregate(initializer, aggregator, aggregator.getMerger(), materialized);
  }

  @Override
  public <K> SerdeFactory<Windowed<K>> getKeySerdeFactory(final Class<K> innerType) {
    return () -> WindowedSerdes.sessionWindowedSerdeFrom(innerType);
  }
}
