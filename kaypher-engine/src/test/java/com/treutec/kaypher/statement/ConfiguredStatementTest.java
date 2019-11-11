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
package com.treutec.kaypher.statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.util.KaypherConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConfiguredStatementTest {

  private static final KaypherConfig CONFIG = new KaypherConfig(ImmutableMap.of());
  @Mock
  private PreparedStatement<? extends Statement> prepared;

  @Test
  public void shouldTakeDefensiveCopyOfProperties() {
    // Given:
    final Map<String, Object> props = new HashMap<>();
    props.put("this", "that");

    final ConfiguredStatement<? extends Statement> statement = ConfiguredStatement
        .of(prepared, props, CONFIG);

    // When:
    props.put("other", "thing");

    // Then:
    assertThat(statement.getOverrides(), is(ImmutableMap.of("this", "that")));
  }

  @Test
  public void shouldReturnImmutableProperties() {
    // Given:
    final ConfiguredStatement<? extends Statement> statement = ConfiguredStatement
        .of(prepared, new HashMap<>(), CONFIG);

    // Then:
    assertThat(statement.getOverrides(), is(instanceOf(ImmutableMap.class)));
  }
}