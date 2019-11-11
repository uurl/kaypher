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
package com.treutec.kaypher.streams;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.execution.streams.GroupedFactory;
import com.treutec.kaypher.util.KaypherConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GroupedFactoryTest {

  private static final String OP_NAME = "kdot";

  @Mock
  private Serde<String> keySerde;
  @Mock
  private Serde<GenericRow> rowSerde;
  @Mock
  private GroupedFactory.Grouper grouper;
  @Mock
  private Grouped<String, GenericRow> grouped;

  @Test
  public void shouldCreateGroupedCorrectlyWhenOptimizationsDisabled() {
    // Given:
    final KaypherConfig kaypherConfig = new KaypherConfig(
        ImmutableMap.of(
            StreamsConfig.TOPOLOGY_OPTIMIZATION,
            StreamsConfig.NO_OPTIMIZATION,
            KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS,
            KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS_OFF)
    );
    when(grouper.groupedWith(null, keySerde, rowSerde)).thenReturn(grouped);

    // When:
    final Grouped returned = GroupedFactory.create(kaypherConfig, grouper).create(
        OP_NAME,
        keySerde,
        rowSerde
    );

    // Then:
    assertThat(returned, is(grouped));
    verify(grouper).groupedWith(null, keySerde, rowSerde);
  }

  @Test
  public void shouldCreateGroupedCorrectlyWhenOptimationsEnabled() {
    // Given:
    final KaypherConfig kaypherConfig = new KaypherConfig(
        ImmutableMap.of(
            KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS,
            KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS_ON)
    );
    when(grouper.groupedWith(OP_NAME, keySerde, rowSerde)).thenReturn(grouped);

    // When:
    final Grouped returned = GroupedFactory.create(kaypherConfig, grouper).create(
        OP_NAME,
        keySerde,
        rowSerde
    );

    // Then:
    assertThat(returned, is(grouped));
    verify(grouper).groupedWith(OP_NAME, keySerde, rowSerde);
  }
}