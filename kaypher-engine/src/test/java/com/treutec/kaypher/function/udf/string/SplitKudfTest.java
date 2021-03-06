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
 */package com.treutec.kaypher.function.udf.string;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class SplitKudfTest {
  private final static SplitKudf splitUdf = new SplitKudf();

  @Test
  public void shouldReturnNullOnAnyNullParameters() {
    assertThat(splitUdf.split(null, ""), is(nullValue()));
    assertThat(splitUdf.split("", null), is(nullValue()));
    assertThat(splitUdf.split(null, null), is(nullValue()));
  }

  @Test
  public void shouldReturnOriginalStringOnNotFoundDelimiter() {
    assertThat(splitUdf.split("", "."), contains(""));
    assertThat(splitUdf.split("x-y", "."), contains("x-y"));
  }

  @Test
  public void shouldSplitAllCharactersByGivenAnEmptyDelimiter() {
    assertThat(splitUdf.split("", ""), contains(""));
    assertThat(splitUdf.split("x-y", ""), contains("x", "-", "y"));
  }

  @Test
  public void shouldSplitStringByGivenDelimiter() {
    assertThat(splitUdf.split("x-y", "-"), contains("x", "y"));
    assertThat(splitUdf.split("x-y", "x"), contains("", "-y"));
    assertThat(splitUdf.split("x-y", "y"), contains("x-", ""));
    assertThat(splitUdf.split("a.b.c.d", "."), contains("a", "b", "c", "d"));

  }

  @Test
  public void shouldSplitAndAddEmptySpacesIfDelimiterIsFoundAtTheBeginningOrEnd() {
    assertThat(splitUdf.split("$A", "$"), contains("", "A"));
    assertThat(splitUdf.split("$A$B", "$"), contains("", "A", "B"));
    assertThat(splitUdf.split("A$", "$"), contains("A", ""));
    assertThat(splitUdf.split("A$B$", "$"), contains("A", "B", ""));
    assertThat(splitUdf.split("$A$B$", "$"), contains("", "A", "B", ""));
  }

  @Test
  public void shouldSplitAndAddEmptySpacesIfDelimiterIsFoundInContiguousPositions() {
    assertThat(splitUdf.split("A||A", "|"), contains("A", "", "A"));
    assertThat(splitUdf.split("z||A||z", "|"), contains("z", "", "A", "", "z"));
    assertThat(splitUdf.split("||A||A", "|"), contains("", "", "A", "", "A"));
    assertThat(splitUdf.split("A||A||", "|"), contains("A", "", "A", "", ""));
  }
}
