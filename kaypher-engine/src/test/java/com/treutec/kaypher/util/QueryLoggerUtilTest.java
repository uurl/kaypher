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
package com.treutec.kaypher.util;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.context.QueryLoggerUtil;
import com.treutec.kaypher.query.QueryId;
import org.junit.Test;

public class QueryLoggerUtilTest {
  private final QueryId queryId = new QueryId("queryid");
  private final QueryContext.Stacker contextStacker = new QueryContext.Stacker();

  @Test
  public void shouldBuildCorrectName() {
    // When:
    final String name = QueryLoggerUtil.queryLoggerName(
        queryId,
        contextStacker.push("biz", "baz").getQueryContext());

    // Then:
    assertThat(name, equalTo("queryid.biz.baz"));
  }

  @Test
  public void shouldBuildCorrectNameWhenNoSubhierarchy() {
    // When:
    final String name = QueryLoggerUtil.queryLoggerName(queryId, contextStacker.getQueryContext());

    // Then:
    assertThat(name, equalTo("queryid"));
  }
}
