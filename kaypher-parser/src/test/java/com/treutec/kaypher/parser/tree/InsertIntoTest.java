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
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.NodeLocation;
import java.util.Optional;
import org.junit.Test;

public class InsertIntoTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final SourceName SOME_NAME = SourceName.of("bob");
  private static final Query SOME_QUERY = mock(Query.class);
  private static final Optional<Expression> SOME_COLUMN = Optional.of(mock(Expression.class));

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new InsertInto(SOME_NAME, SOME_QUERY, SOME_COLUMN),
            new InsertInto(SOME_NAME, SOME_QUERY, SOME_COLUMN),
            new InsertInto(Optional.of(SOME_LOCATION), SOME_NAME, SOME_QUERY, SOME_COLUMN),
            new InsertInto(Optional.of(OTHER_LOCATION), SOME_NAME, SOME_QUERY, SOME_COLUMN)
        )
        .addEqualityGroup(
            new InsertInto(SourceName.of("jim"), SOME_QUERY, SOME_COLUMN)
        )
        .addEqualityGroup(
            new InsertInto(SOME_NAME, mock(Query.class), SOME_COLUMN)
        )
        .addEqualityGroup(
            new InsertInto(SOME_NAME, SOME_QUERY, Optional.empty())
        )
        .testEquals();
  }
}