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
package com.treutec.kaypher.internal;

import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.metrics.MeasurableStat;

public final class KaypherMetric {
  private final String name;
  private final String description;
  private final Supplier<MeasurableStat> statSupplier;

  public static KaypherMetric of(
      final String name,
      final String description,
      final Supplier<MeasurableStat> statSupplier) {
    return new KaypherMetric(name, description, statSupplier);
  }

  private KaypherMetric(
      final String name,
      final String description,
      final Supplier<MeasurableStat> statSupplier) {
    this.name = Objects.requireNonNull(name, "name cannot be null");
    this.description = Objects.requireNonNull(description, "description cannot be null");
    this.statSupplier = Objects.requireNonNull(statSupplier, "statSupplier cannot be null");
  }

  public String name() {
    return name;
  }

  public String description() {
    return description;
  }

  public Supplier<MeasurableStat> statSupplier() {
    return statSupplier;
  }
}