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
import static org.hamcrest.Matchers.*;

import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import java.util.Optional;
import org.junit.Test;

public class ColumnReferenceParserTest {

  @Test
  public void shouldParseUnquotedIdentifier() {
    // When:
    final ColumnRef result = ColumnReferenceParser.parse("foo");

    // Then:
    assertThat(result.source(), is(Optional.empty()));
    assertThat(result.name(), is(ColumnName.of("FOO")));
  }

  @Test
  public void shouldParseUnquotedColumnRef() {
    // When:
    final ColumnRef result = ColumnReferenceParser.parse("a.foo");

    // Then:
    assertThat(result.source(), is(Optional.of(SourceName.of("A"))));
    assertThat(result.name(), is(ColumnName.of("FOO")));
  }

  @Test
  public void shouldParseQuotedIdentifier() {
    // When:
    final ColumnRef result = ColumnReferenceParser.parse("`foo`");

    // Then:
    assertThat(result.source(), is(Optional.empty()));
    assertThat(result.name(), is(ColumnName.of("foo")));
  }

  @Test
  public void shouldParseQuotedIdentifierWithDot() {
    // When:
    final ColumnRef result = ColumnReferenceParser.parse("`foo.bar`");

    // Then:
    assertThat(result.source(), is(Optional.empty()));
    assertThat(result.name(), is(ColumnName.of("foo.bar")));
  }


  @Test
  public void shouldParseQuotedColumnRef() {
    // When:
    final ColumnRef result = ColumnReferenceParser.parse("a.`foo`");

    // Then:
    assertThat(result.source(), is(Optional.of(SourceName.of("A"))));
    assertThat(result.name(), is(ColumnName.of("foo")));
  }


}