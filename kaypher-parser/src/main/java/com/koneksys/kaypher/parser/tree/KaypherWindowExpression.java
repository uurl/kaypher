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
import com.koneksys.kaypher.GenericRow;
import com.koneksys.kaypher.function.UdafAggregator;
import com.koneksys.kaypher.metastore.SerdeFactory;
import java.util.Optional;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Windowed;

@Immutable
public abstract class KaypherWindowExpression extends Node {

  KaypherWindowExpression(final Optional<NodeLocation> location) {
    super(location);
  }

  public abstract KTable applyAggregate(KGroupedStream groupedStream,
                                        Initializer initializer,
                                        UdafAggregator aggregator,
                                        Materialized<String, GenericRow, ?> materialized);

  public abstract <K> SerdeFactory<Windowed<K>> getKeySerdeFactory(Class<K> innerType);

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitKaypherWindowExpression(this, context);
  }
}
