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
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import com.treutec.kaypher.embedded.KaypherContext;
import com.treutec.kaypher.embedded.KaypherContextTestUtil;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.util.KaypherConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KaypherContextTestUtilTest {

  private static final KaypherConfig BASE_CONFIG = new KaypherConfig(ImmutableMap.of(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:10"
  ));

  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private FunctionRegistry functionRegistry;

  @SuppressWarnings("unused")
  @Test
  public void shouldBeAbleToHaveTwoInstancesWithDifferentNames() {
    // Given:
    try (KaypherContext first = KaypherContextTestUtil.create(
        BASE_CONFIG,
        srClient,
        functionRegistry
    )) {
      first.terminateQuery(new QueryId("avoid compiler warning"));

      // When:
      KaypherContext second = KaypherContextTestUtil.create(
          BASE_CONFIG,
          srClient,
          functionRegistry
      );

      // Then: did not throw due to metrics name clash.
      second.close();
    }
  }
}