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
package com.treutec.kaypher.function.udaf.map;

import com.google.common.collect.Maps;
import com.treutec.kaypher.function.udaf.TableUdaf;
import com.treutec.kaypher.function.udaf.UdafDescription;
import com.treutec.kaypher.function.udaf.UdafFactory;
import java.util.Map;

@UdafDescription(name = "histogram",
    description = "Returns a map of each distinct String from the"
    + " input Stream or Table and how many times each occurs."
    + " \nThis version limits the size of the resultant Map to 1000 entries. Any entries added"
    + " beyond this limit will be ignored.")
public final class HistogramUdaf {

  private static final int LIMIT = 1000;
  
  private HistogramUdaf() {
  }
  
  private static <T> TableUdaf<T, Map<T, Long>, Map<T, Long>> histogram() {
    return new TableUdaf<T, Map<T, Long>, Map<T, Long>>() {

      @Override
      public Map<T, Long> initialize() {
        return Maps.newHashMap();
      }

      @Override
      public Map<T, Long> aggregate(final T current, final Map<T, Long> aggregate) {
        if (aggregate.size() < LIMIT || aggregate.containsKey(current)) {
          aggregate.merge(current, 1L, Long::sum);
        }
        return aggregate;
      }

      @Override
      public Map<T, Long> merge(final Map<T, Long> agg1, final Map<T, Long> agg2) {
        agg2.forEach((k, v) -> {
          if (agg1.size() < LIMIT || agg1.containsKey(k)) {
            agg1.merge(k, v, Long::sum);
          }
        });
        return agg1;
      }

      @Override
      public Map<T, Long> undo(final T valueToUndo, final Map<T, Long> aggregate) {
        aggregate.compute(valueToUndo, (k, v) -> (--v < 1) ? null : v);
        return aggregate;
      }

      @Override
      public Map<T, Long> map(final Map<T, Long> aggregate) {
        return aggregate;
      }

    };
  }

  @UdafFactory(description = "Build a value-to-count histogram of input Strings")
  public static TableUdaf<String, Map<String, Long>, Map<String, Long>> histogramString() {
    return histogram();
  }

}
