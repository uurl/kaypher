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

import com.google.common.testing.EqualsTester;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.NodeLocation;
import java.util.Optional;
import org.junit.Test;


public class AliasedRelationTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final SourceName SOME_NAME = SourceName.of("alias");

  private static final Relation SOME_RELATION = new Table(SourceName.of("bob"));
  private static final Relation OTHER_RELATION = new Table(SourceName.of("pete"));

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new AliasedRelation(SOME_RELATION, SOME_NAME),
            new AliasedRelation(SOME_RELATION, SOME_NAME),
            new AliasedRelation(Optional.of(SOME_LOCATION), SOME_RELATION, SOME_NAME),
            new AliasedRelation(Optional.of(OTHER_LOCATION), SOME_RELATION, SOME_NAME)
        )
        .addEqualityGroup(
            new AliasedRelation(OTHER_RELATION, SOME_NAME)
        )
        .addEqualityGroup(
            new AliasedRelation(SOME_RELATION, SourceName.of("other"))
        )
        .testEquals();
  }
}