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
package com.treutec.kaypher.function.udf.url;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.treutec.kaypher.util.KaypherException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class UrlExtractPathKudfTest {

  private UrlExtractPathKudf extractUdf;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    extractUdf = new UrlExtractPathKudf();
  }

  @Test
  public void shouldExtractPathIfPresent() {
    assertThat(extractUdf.extractPath("https://docs.confluent.io/current/kaypher/docs/syntax-reference.html#scalar-functions"), equalTo("/current/kaypher/docs/syntax-reference.html"));
  }

  @Test
  public void shouldReturnSlashIfRootPath() {
    assertThat(extractUdf.extractPath("https://docs.confluent.io/"), equalTo("/"));
  }

  @Test
  public void shouldReturnEmptyIfNoPath() {
    assertThat(extractUdf.extractPath("https://docs.confluent.io"), equalTo(""));
  }

  @Test
  public void shouldThrowExceptionForMalformedURL() {
    // Given:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("URL input has invalid syntax: http://257.1/bogus/[url");

    // When:
    extractUdf.extractPath("http://257.1/bogus/[url");
  }
}
