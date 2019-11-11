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

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import com.treutec.kaypher.execution.expression.tree.StringLiteral;
import com.treutec.kaypher.execution.expression.tree.Type;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.NodeLocation;
import com.treutec.kaypher.parser.properties.with.CreateSourceProperties;
import com.treutec.kaypher.parser.tree.TableElement.Namespace;
import com.treutec.kaypher.properties.with.CommonCreateConfigs;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import java.util.Optional;
import org.junit.Test;

public class CreateStreamTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);
  private static final SourceName SOME_NAME = SourceName.of("bob");
  private static final TableElements SOME_ELEMENTS = TableElements.of(
      new TableElement(Namespace.VALUE, ColumnName.of("Bob"), new Type(SqlTypes.STRING))
  );
  private static final CreateSourceProperties SOME_PROPS = CreateSourceProperties.from(
      ImmutableMap.of(
      "value_format", new StringLiteral("json"),
      "kafka_topic", new StringLiteral("foo"))
  );
  private static final CreateSourceProperties OTHER_PROPS = CreateSourceProperties.from(
      ImmutableMap.of(
          "value_format", new StringLiteral("json"),
          "kafka_topic", new StringLiteral("foo"),
          CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY, new StringLiteral("foo"))
  );

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new CreateStream(SOME_NAME, SOME_ELEMENTS, true, SOME_PROPS),
            new CreateStream(SOME_NAME, SOME_ELEMENTS, true, SOME_PROPS),
            new CreateStream(Optional.of(SOME_LOCATION), SOME_NAME, SOME_ELEMENTS, true, SOME_PROPS),
            new CreateStream(Optional.of(OTHER_LOCATION), SOME_NAME, SOME_ELEMENTS, true, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateStream(SourceName.of("jim"), SOME_ELEMENTS, true, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateStream(SOME_NAME, TableElements.of(), true, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateStream(SOME_NAME, SOME_ELEMENTS, false, SOME_PROPS)
        )
        .addEqualityGroup(
            new CreateStream(SOME_NAME, SOME_ELEMENTS, true, OTHER_PROPS)
        )
        .testEquals();
  }
}