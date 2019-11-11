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
package com.treutec.kaypher.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.treutec.kaypher.execution.expression.tree.BooleanLiteral;
import com.treutec.kaypher.execution.expression.tree.LongLiteral;
import com.treutec.kaypher.execution.expression.tree.StringLiteral;
import com.treutec.kaypher.util.KaypherException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LiteralUtilTest {

  @Rule
  public final ExpectedException expectedExcetion = ExpectedException.none();

  @Test
  public void shouldConvertBooleanToBoolean() {
    assertThat(LiteralUtil.toBoolean(new BooleanLiteral("true"), "bob"), is(true));
    assertThat(LiteralUtil.toBoolean(new BooleanLiteral("false"), "bob"), is(false));
  }

  @Test
  public void shouldConvertStringContainingBooleanToBoolean() {
    assertThat(LiteralUtil.toBoolean(new StringLiteral("tRuE"), "bob"), is(true));
    assertThat(LiteralUtil.toBoolean(new StringLiteral("faLSe"), "bob"), is(false));
  }

  @Test
  public void shouldThrowConvertingOtherLiteralTypesToBoolean() {
    // Then:
    expectedExcetion.expect(KaypherException.class);
    expectedExcetion.expectMessage("Property 'bob' is not a boolean value");

    // When:
    LiteralUtil.toBoolean(new LongLiteral(10), "bob");
  }
}