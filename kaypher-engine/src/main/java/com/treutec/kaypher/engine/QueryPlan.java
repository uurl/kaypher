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
package com.treutec.kaypher.engine;

import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.physical.PhysicalPlan;
import java.util.Objects;
import java.util.Set;

public final class QueryPlan  {
  private final Set<SourceName> sources;
  private final SourceName sink;
  private final PhysicalPlan<?> physicalPlan;

  public QueryPlan(
      final Set<SourceName> sources,
      final SourceName sink,
      final PhysicalPlan<?> physicalPlan
  ) {
    this.sources = Objects.requireNonNull(sources, "sources");
    this.sink = Objects.requireNonNull(sink, "sink");
    this.physicalPlan = Objects.requireNonNull(physicalPlan, "physicalPlan");
  }

  public SourceName getSink() {
    return sink;
  }

  public Set<SourceName> getSources() {
    return sources;
  }

  public PhysicalPlan<?> getPhysicalPlan() {
    return physicalPlan;
  }
}
