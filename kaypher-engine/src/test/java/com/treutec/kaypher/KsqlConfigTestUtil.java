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

import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.test.util.EmbeddedSingleNodeKafkaCluster;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;

public final class KaypherConfigTestUtil {

  private static final Map<String, Object> BASE_CONFIG = ImmutableMap.of(
      "commit.interval.ms", 0,
      "cache.max.bytes.buffering", 0,
      "auto.offset.reset", "earliest",
      StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()
  );

  private KaypherConfigTestUtil() {
  }

  public static Map<String, Object> baseTestConfig() {
    return BASE_CONFIG;
  }

  public static KaypherConfig create(final EmbeddedSingleNodeKafkaCluster kafkaCluster) {
    return create(kafkaCluster, Collections.emptyMap());
  }

  public static KaypherConfig create(
      final EmbeddedSingleNodeKafkaCluster kafkaCluster,
      final Map<String, Object> additionalConfig
  ) {
    final ImmutableMap<String, Object> config = ImmutableMap.<String, Object>builder()
        .putAll(kafkaCluster.getClientProperties())
        .putAll(additionalConfig)
        .build();
    return create(kafkaCluster.bootstrapServers(), config);
  }

  public static KaypherConfig create(
      final String kafkaBootstrapServers
  ) {
    return create(kafkaBootstrapServers, ImmutableMap.of());
  }

  public static KaypherConfig create(
      final String kafkaBootstrapServers,
      final Map<String, Object> additionalConfig
  ) {
    final Map<String, Object> config = new HashMap<>(BASE_CONFIG);
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    config.putAll(additionalConfig);
    return new KaypherConfig(config);
  }
}
