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
package com.treutec.kaypher.util.timestamp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.treutec.kaypher.util.KaypherConstants;
import com.treutec.kaypher.util.KaypherException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PartialStringToTimestampParserTest {

  private static final StringToTimestampParser FULL_PARSER =
      new StringToTimestampParser(KaypherConstants.DATE_TIME_PATTERN + "X");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private PartialStringToTimestampParser parser;

  @Before
  public void init() {
    parser = new PartialStringToTimestampParser();
  }

  @Test
  public void shouldParseYear() {
    // When:
    assertThat(parser.parse("2017"), is(fullParse("2017-01-01T00:00:00.000+0000")));
  }

  @Test
  public void shouldParseYearMonth() {
    // When:
    assertThat(parser.parse("2020-02"), is(fullParse("2020-02-01T00:00:00.000+0000")));
  }

  @Test
  public void shouldParseFullDate() {
    // When:
    assertThat(parser.parse("2020-01-02"), is(fullParse("2020-01-02T00:00:00.000+0000")));
    assertThat(parser.parse("2020-01-02T"), is(fullParse("2020-01-02T00:00:00.000+0000")));
  }

  @Test
  public void shouldParseDateWithHour() {
    // When:
    assertThat(parser.parse("2020-12-02T13"), is(fullParse("2020-12-02T13:00:00.000+0000")));
  }

  @Test
  public void shouldParseDateWithHourMinute() {
    // When:
    assertThat(parser.parse("2020-12-02T13:59"), is(fullParse("2020-12-02T13:59:00.000+0000")));
  }

  @Test
  public void shouldParseDateWithHourMinuteSecond() {
    // When:
    assertThat(parser.parse("2020-12-02T13:59:58"), is(fullParse("2020-12-02T13:59:58.000+0000")));
  }

  @Test
  public void shouldParseFullDateTime() {
    // When:
    assertThat(parser.parse("2020-12-02T13:59:58.123"), is(fullParse("2020-12-02T13:59:58.123+0000")));
  }

  @Test
  public void shouldParseDateTimeWithPositiveTimezones() {
    assertThat(parser.parse("2017-11-13T23:59:58.999+0100"), is(1510613998999L));
  }

  @Test
  public void shouldParseDateTimeWithNegativeTimezones() {
    assertThat(parser.parse("2017-11-13T23:59:58.999-0100"), is(1510621198999L));
  }

  @Test
  public void shouldThrowOnIncorrectlyFormattedDateTime() {
    // Expect:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Failed to parse timestamp '2017-1-1'");

    // When:
    parser.parse("2017-1-1");
  }

  @Test
  public void shouldThrowOnTimezoneParseError() {
    // Expect:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Failed to parse timestamp '2017-01-01T00:00:00.000+foo'");

    // When:
    parser.parse("2017-01-01T00:00:00.000+foo");
  }

  @Test
  public void shouldIncludeRequiredFormatInErrorMessage() {
    // Expect:
    expectedException.expectMessage("Required format is: \"yyyy-MM-dd'T'HH:mm:ss.SSS\", "
        + "with an optional numeric 4-digit timezone");

    // When:
    parser.parse("2017-01-01T00:00:00.000+foo");
  }

  private static long fullParse(final String text) {
    return FULL_PARSER.parse(text);
  }
}