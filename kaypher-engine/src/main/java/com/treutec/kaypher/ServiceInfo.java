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
package com.treutec.kaypher;

import com.treutec.kaypher.internal.KaypherMetricsExtension;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class ServiceInfo {

  private final String serviceId;
  private final String metricsPrefix;
  private final Map<String, String> customMetricsTags;
  private final Optional<KaypherMetricsExtension> metricsExtension;

  /**
   * Create ServiceInfo required by KAYPHER engine.
   *
   * @param kaypherConfig the server config.
   * @return new instance.
   */
  public static ServiceInfo create(final KaypherConfig kaypherConfig) {
    return create(kaypherConfig, "");
  }

  /**
   * Create ServiceInfo required by KAYPHER engine.
   *
   * @param kaypherConfig the server config.
   * @param metricsPrefix optional prefix for metrics group names. Default is empty string.
   * @return new instance.
   */
  public static ServiceInfo create(
      final KaypherConfig kaypherConfig,
      final String metricsPrefix
  ) {
    final String serviceId = kaypherConfig.getString(KaypherConfig.KAYPHER_SERVICE_ID_CONFIG);
    final Map<String, String> customMetricsTags =
        kaypherConfig.getStringAsMap(KaypherConfig.KAYPHER_CUSTOM_METRICS_TAGS);
    final Optional<KaypherMetricsExtension> metricsExtension = Optional.ofNullable(
        kaypherConfig.getConfiguredInstance(
            KaypherConfig.KAYPHER_CUSTOM_METRICS_EXTENSION,
            KaypherMetricsExtension.class
        ));

    return new ServiceInfo(serviceId, customMetricsTags, metricsExtension, metricsPrefix);
  }

  private ServiceInfo(
      final String serviceId,
      final Map<String, String> customMetricsTags,
      final Optional<KaypherMetricsExtension> metricsExtension,
      final String metricsPrefix
  ) {
    this.serviceId = Objects.requireNonNull(serviceId, "serviceId");
    this.customMetricsTags = Objects.requireNonNull(customMetricsTags, "customMetricsTags");
    this.metricsExtension = Objects.requireNonNull(metricsExtension, "metricsExtension");
    this.metricsPrefix = Objects.requireNonNull(metricsPrefix, "metricsPrefix");
  }

  public String serviceId() {
    return serviceId;
  }

  public Map<String, String> customMetricsTags() {
    return customMetricsTags;
  }

  public Optional<KaypherMetricsExtension> metricsExtension() {
    return metricsExtension;
  }

  public String metricsPrefix() {
    return metricsPrefix;
  }
}
