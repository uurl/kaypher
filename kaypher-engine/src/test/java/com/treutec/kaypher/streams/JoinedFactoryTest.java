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
import com.treutec.kaypher.execution.streams.JoinedFactory;
import com.treutec.kaypher.util.KaypherConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Joined;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JoinedFactoryTest {

  private static final String OP_NAME = "kdot";

  @Mock
  private Serde<String> keySerde;
  @Mock
  private Serde<GenericRow> leftSerde;
  @Mock
  private Serde<GenericRow> rightSerde;
  @Mock
  private JoinedFactory.Joiner joiner;
  @Mock
  private Joined<String, GenericRow, GenericRow> joined;

  @Test
  public void shouldCreateJoinedCorrectlyWhenOptimizationsDisabled() {
    // Given:
    final KaypherConfig kaypherConfig = new KaypherConfig(
        ImmutableMap.of(
            StreamsConfig.TOPOLOGY_OPTIMIZATION,
            StreamsConfig.NO_OPTIMIZATION,
            KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS,
            KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS_OFF)
    );
    when(joiner.joinedWith(keySerde, leftSerde, rightSerde, null)).thenReturn(joined);

    // When:
    final Joined<String, GenericRow, GenericRow> returned
        = JoinedFactory.create(kaypherConfig, joiner).create(
        keySerde, leftSerde, rightSerde, OP_NAME);

    // Then:
    assertThat(returned, is(joined));
    verify(joiner).joinedWith(keySerde, leftSerde, rightSerde, null);
  }

  @Test
  public void shouldCreateJoinedCorrectlyWhenOptimizationsEnabled() {
    // Given:
    final KaypherConfig kaypherConfig = new KaypherConfig(
        ImmutableMap.of(
            KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS,
            KaypherConfig.KAYPHER_USE_NAMED_INTERNAL_TOPICS_ON)
    );
    when(joiner.joinedWith(keySerde, leftSerde, rightSerde, OP_NAME)).thenReturn(joined);

    // When:
    final Joined<String, GenericRow, GenericRow> returned
        = JoinedFactory.create(kaypherConfig, joiner).create(
        keySerde, leftSerde, rightSerde, OP_NAME);

    // Then:
    assertThat(returned, is(joined));
    verify(joiner).joinedWith(keySerde, leftSerde, rightSerde, OP_NAME);
  }
}