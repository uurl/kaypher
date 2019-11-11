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
package com.treutec.kaypher.structured;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableSet;
import com.treutec.kaypher.execution.context.QueryContext;
import org.junit.Test;

public class QueryContextTest {
  private final QueryContext.Stacker contextStacker = new QueryContext.Stacker().push("node");
  private final QueryContext queryContext = contextStacker.getQueryContext();

  private static void assertQueryContext(
      final QueryContext queryContext,
      final String ...context) {
    if (context.length > 0) {
      assertThat(queryContext.getContext(), contains(context));
    } else {
      assertThat(queryContext.getContext(), empty());
    }
  }

  @Test
  public void shouldGenerateNewContextOnPush() {
    // When:
    final QueryContext.Stacker childContextStacker = contextStacker.push("child");
    final QueryContext childContext = childContextStacker.getQueryContext();
    final QueryContext grandchildContext = childContextStacker.push("grandchild").getQueryContext();

    // Then:
    assertThat(ImmutableSet.of(queryContext, childContext, grandchildContext), hasSize(3));
    assertQueryContext(queryContext, "node");
    assertQueryContext(childContext, "node", "child");
    assertQueryContext(grandchildContext, "node", "child", "grandchild");
  }
}