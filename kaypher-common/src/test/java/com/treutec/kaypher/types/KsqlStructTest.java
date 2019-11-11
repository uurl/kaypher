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
package com.treutec.kaypher.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.inOrder;

import com.treutec.kaypher.schema.kaypher.DataException;
import com.treutec.kaypher.schema.kaypher.types.Field;
import com.treutec.kaypher.schema.kaypher.types.SqlStruct;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.util.KaypherException;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KaypherStructTest {

  private static final SqlStruct SCHEMA = SqlTypes.struct()
      .field("f0", SqlTypes.BIGINT)
      .field(Field.of("v1", SqlTypes.BOOLEAN))
      .build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private BiConsumer<? super Field, ? super Optional<?>> consumer;

  @Test
  public void shouldHandleExplicitNulls() {
    // When:
    final KaypherStruct struct = KaypherStruct.builder(SCHEMA)
        .set("f0", Optional.empty())
        .build();

    // Then:
    assertThat(struct.values(), contains(Optional.empty(), Optional.empty()));
  }

  @Test
  public void shouldHandleImplicitNulls() {
    // When:
    final KaypherStruct struct = KaypherStruct.builder(SCHEMA)
        .build();

    // Then:
    assertThat(struct.values(), contains(Optional.empty(), Optional.empty()));
  }

  @Test
  public void shouldThrowFieldNotKnown() {
    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Unknown field: ??");

    // When:
    KaypherStruct.builder(SCHEMA)
        .set("??", Optional.empty());
  }

  @Test
  public void shouldThrowIfValueWrongType() {
    // Then:
    expectedException.expect(DataException.class);
    expectedException.expectMessage("Expected BIGINT, got STRING");

    // When:
    KaypherStruct.builder(SCHEMA)
        .set("f0", Optional.of("field is BIGINT, so won't like this"));
  }

  @Test
  public void shouldBuildStruct() {
    // When:
    final KaypherStruct struct = KaypherStruct.builder(SCHEMA)
        .set("f0", Optional.of(10L))
        .set("v1", Optional.of(true))
        .build();

    // Then:
    assertThat(struct.values(), contains(Optional.of(10L), Optional.of(true)));
  }

  @Test
  public void shouldVisitFieldsInOrder() {
    // Given:
    final KaypherStruct struct = KaypherStruct.builder(SCHEMA)
        .set("f0", Optional.of(10L))
        .set("v1", Optional.of(true))
        .build();

    // When:
    struct.forEach(consumer);

    // Then:
    final InOrder inOrder = inOrder(consumer);
    inOrder.verify(consumer).accept(
        struct.schema().fields().get(0),
        struct.values().get(0)
    );
    inOrder.verify(consumer).accept(
        struct.schema().fields().get(1),
        struct.values().get(1)
    );
  }
}