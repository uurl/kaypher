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

import static com.treutec.kaypher.serde.Format.AVRO;
import static com.treutec.kaypher.serde.Format.JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.Optional;
import org.junit.Test;

public class ValueFormatTest {

  private static final FormatInfo FORMAT_INFO =
      FormatInfo.of(
          AVRO,
          Optional.of("something"),
          Optional.empty()
      );

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .testAllPublicStaticMethods(ValueFormat.class);
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            ValueFormat.of(FORMAT_INFO),
            ValueFormat.of(FORMAT_INFO)
        )
        .addEqualityGroup(
            ValueFormat.of(FormatInfo.of(JSON, Optional.empty(), Optional.empty()))
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToString() {
    // Given:
    final ValueFormat valueFormat = ValueFormat.of(FORMAT_INFO);

    // When:
    final String result = valueFormat.toString();

    // Then:
    assertThat(result, containsString(FORMAT_INFO.toString()));
  }

  @Test
  public void shouldGetFormat() {
    // Given:
    final ValueFormat valueFormat = ValueFormat.of(FORMAT_INFO);

    // When:
    final Format result = valueFormat.getFormat();

    // Then:
    assertThat(result, is(FORMAT_INFO.getFormat()));
  }

  @Test
  public void shouldGetFormatInfo() {
    // Given:
    final ValueFormat valueFormat = ValueFormat.of(FORMAT_INFO);

    // When:
    final FormatInfo result = valueFormat.getFormatInfo();

    // Then:
    assertThat(result, is(FORMAT_INFO));
  }
}