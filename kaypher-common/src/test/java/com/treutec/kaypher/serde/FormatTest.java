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
package com.treutec.kaypher.serde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.treutec.kaypher.util.KaypherException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FormatTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldCreateFromString() {
    assertThat(Format.of("JsoN"), is(Format.JSON));
    assertThat(Format.of("AvRo"), is(Format.AVRO));
    assertThat(Format.of("Delimited"), is(Format.DELIMITED));
  }

  @Test
  public void shouldHaveCorrectToString() {
    assertThat(Format.JSON.toString(), is("JSON"));
    assertThat(Format.AVRO.toString(), is("AVRO"));
    assertThat(Format.DELIMITED.toString(), is("DELIMITED"));
  }

  @Test
  public void shouldThrowOnUnknownFormat() {
    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Unknown format: bob");

    // When:
    Format.of("bob");
  }
}