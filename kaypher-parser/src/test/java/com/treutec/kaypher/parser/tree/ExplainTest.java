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
package com.treutec.kaypher.parser.tree;

import static org.mockito.Mockito.mock;

import com.google.common.testing.EqualsTester;
import com.treutec.kaypher.parser.NodeLocation;
import java.util.Optional;
import org.junit.Test;

public class ExplainTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final Optional<Statement> SOME_STATEMENT = Optional.of(mock(Statement.class));
  private static final Optional<String> SOME_QUERY_ID = Optional.of("bob");

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new Explain(SOME_QUERY_ID, Optional.empty()),
            new Explain(SOME_QUERY_ID, Optional.empty()),
            new Explain(Optional.of(OTHER_LOCATION), SOME_QUERY_ID, Optional.empty()),
            new Explain(Optional.of(SOME_LOCATION), SOME_QUERY_ID, Optional.empty())
        )
        .addEqualityGroup(
            new Explain(Optional.empty(), SOME_STATEMENT)
        )
        .testEquals();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfNeitherSet() {
    new Explain(Optional.empty(), Optional.empty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfBothSet() {
    new Explain(SOME_QUERY_ID, SOME_STATEMENT);
  }
}