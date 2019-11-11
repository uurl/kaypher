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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.treutec.kaypher.util.KaypherException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class UrlExtractParameterKudfTest {

  private UrlExtractParameterKudf extractUdf;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    extractUdf = new UrlExtractParameterKudf();
  }

  @Test
  public void shouldExtractParamValueIfPresent() {
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz%20zab&blank#scalar-functions", "foo bar"), equalTo("baz zab"));
  }

  @Test
  public void shouldExtractParamValueCaseSensitive() {
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz&blank#scalar-functions", "foo Bar"), nullValue());
  }

  @Test
  public void shouldReturnEmptyStringIfParamHasNoValue() {
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz&blank#scalar-functions", "blank"), equalTo(""));
  }

  @Test
  public void shouldReturnNullIfParamNotPresent() {
    assertThat(extractUdf.extractParam("https://docs.confluent.io?foo%20bar=baz&blank#scalar-functions", "absent"), nullValue());
  }

  @Test
  public void shouldThrowExceptionForMalformedURL() {
    // Given:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("URL input has invalid syntax: http://257.1/bogus/[url");

    // When:
    extractUdf.extractParam("http://257.1/bogus/[url", "foo bar");
  }

}
