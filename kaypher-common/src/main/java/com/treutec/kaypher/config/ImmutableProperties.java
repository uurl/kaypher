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


package com.treutec.kaypher.config;

import com.google.common.collect.ImmutableSet;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.Set;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Hard coded list of known immutable properties
 */
public final class ImmutableProperties {

  private static final Set<String> IMMUTABLE_PROPERTIES = ImmutableSet.<String>builder()
      .add(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)
      .add(KaypherConfig.KAYPHER_EXT_DIR)
      .add(KaypherConfig.KAYPHER_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG)
      .add(KaypherConfig.KAYPHER_PULL_QUERIES_ENABLE_CONFIG)
      .addAll(KaypherConfig.SSL_CONFIG_NAMES)
      .build();

  private ImmutableProperties() {
  }

  public static Set<String> getImmutableProperties() {
    return IMMUTABLE_PROPERTIES;
  }
}
