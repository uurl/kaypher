/*
 * Copyright 2019 Koneksys
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

package com.koneksys.kaypher.parser.tree;

import static com.koneksys.kaypher.parser.tree.Join.Type.INNER;
import static com.koneksys.kaypher.parser.tree.Join.Type.OUTER;

import com.google.common.testing.EqualsTester;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class JoinTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final Relation RELATION_0 = new Table(QualifiedName.of("bob"));
  private static final Relation RELATION_1 = new Table(QualifiedName.of("pete"));
  private static final JoinCriteria SOME_CRITERIA = new JoinOn(new StringLiteral("j"));
  private static final JoinCriteria OTHER_CRITERIA = new JoinOn(new StringLiteral("p"));
  private static final Optional<WithinExpression> SOME_WITHIN =
      Optional.of(new WithinExpression(1, TimeUnit.SECONDS));

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new Join(INNER, RELATION_0, RELATION_1, SOME_CRITERIA, SOME_WITHIN),
            new Join(INNER, RELATION_0, RELATION_1, SOME_CRITERIA, SOME_WITHIN),
            new Join(Optional.of(SOME_LOCATION), INNER, RELATION_0, RELATION_1, SOME_CRITERIA,
                SOME_WITHIN),
            new Join(Optional.of(OTHER_LOCATION), INNER, RELATION_0, RELATION_1, SOME_CRITERIA,
                SOME_WITHIN)
        )
        .addEqualityGroup(
            new Join(OUTER, RELATION_0, RELATION_1, SOME_CRITERIA, SOME_WITHIN)
        )
        .addEqualityGroup(
            new Join(INNER, RELATION_1, RELATION_1, SOME_CRITERIA, SOME_WITHIN)
        )
        .addEqualityGroup(
            new Join(INNER, RELATION_0, RELATION_0, SOME_CRITERIA, SOME_WITHIN)
        )
        .addEqualityGroup(
            new Join(INNER, RELATION_0, RELATION_1, OTHER_CRITERIA, SOME_WITHIN)
        )
        .addEqualityGroup(
            new Join(INNER, RELATION_0, RELATION_1, SOME_CRITERIA, Optional.empty())
        )
        .testEquals();
  }
}