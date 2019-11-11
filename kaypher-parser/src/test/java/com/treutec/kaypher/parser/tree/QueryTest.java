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

import static com.treutec.kaypher.parser.tree.ResultMaterialization.CHANGES;
import static com.treutec.kaypher.parser.tree.ResultMaterialization.FINAL;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.parser.NodeLocation;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Test;


public class QueryTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final Relation OTHER_RELATION = new Table(SourceName.of("pete"));
  private static final Select SOME_SELECT = new Select(ImmutableList.of(
      new AllColumns(Optional.empty())));
  private static final Select OTHER_SELECT = new Select(ImmutableList.of(new SingleColumn(
      new ColumnReferenceExp(ColumnRef.withoutSource(ColumnName.of("Bob"))),
      Optional.of(ColumnName.of("B")))));
  private static final Relation SOME_FROM = new Table(SourceName.of("from"));
  private static final Optional<WindowExpression> SOME_WINDOW = Optional.of(
      mock(WindowExpression.class)
  );
  private static final Optional<Expression> SOME_WHERE = Optional.of(
      mock(Expression.class)
  );
  private static final Optional<GroupBy> SOME_GROUP_BY = Optional.of(
      mock(GroupBy.class)
  );
  private static final Optional<Expression> SOME_HAVING = Optional.of(
      mock(Expression.class)
  );
  private static final OptionalInt SOME_LIMIT = OptionalInt.of(1);

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            new Query(Optional.empty(), SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, FINAL, true, SOME_LIMIT),
            new Query(Optional.empty(), SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, FINAL, true, SOME_LIMIT),
            new Query(Optional.of(OTHER_LOCATION), SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, FINAL, true, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(Optional.empty(), OTHER_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, FINAL, true, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(Optional.empty(), SOME_SELECT, OTHER_RELATION, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, FINAL, true, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(Optional.empty(), SOME_SELECT, SOME_FROM, Optional.empty(), SOME_WHERE,
                SOME_GROUP_BY, SOME_HAVING, FINAL, true, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(Optional.empty(), SOME_SELECT, SOME_FROM, SOME_WINDOW,
                Optional.empty(),
                SOME_GROUP_BY, SOME_HAVING, FINAL, true, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(Optional.empty(), SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, Optional.empty(), SOME_HAVING, FINAL, true, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(Optional.empty(), SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, Optional.empty(), FINAL, true, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(Optional.empty(), SOME_SELECT, SOME_FROM, SOME_WINDOW, SOME_WHERE,
                SOME_GROUP_BY, SOME_HAVING, CHANGES, true, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(Optional.empty(), SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, FINAL, false, SOME_LIMIT)
        )
        .addEqualityGroup(
            new Query(Optional.empty(), SOME_SELECT, SOME_FROM, SOME_WINDOW,
                SOME_WHERE, SOME_GROUP_BY, SOME_HAVING, FINAL, true, OptionalInt.empty())
        )
        .testEquals();
  }
}