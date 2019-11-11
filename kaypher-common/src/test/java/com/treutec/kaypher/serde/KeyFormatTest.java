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

import static com.treutec.kaypher.model.WindowType.HOPPING;
import static com.treutec.kaypher.model.WindowType.SESSION;
import static com.treutec.kaypher.serde.Format.AVRO;
import static com.treutec.kaypher.serde.Format.DELIMITED;
import static com.treutec.kaypher.serde.Format.JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.time.Duration;
import java.util.Optional;
import org.junit.Test;

public class KeyFormatTest {

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(FormatInfo.class, mock(FormatInfo.class))
        .setDefault(WindowInfo.class, mock(WindowInfo.class))
        .testAllPublicStaticMethods(KeyFormat.class);
  }

  @Test
  public void shouldImplementEquals() {

    final FormatInfo format1 = FormatInfo.of(AVRO, Optional.empty(), Optional.empty());
    final FormatInfo format2 = FormatInfo.of(JSON, Optional.empty(), Optional.empty());

    final WindowInfo window1 = WindowInfo.of(SESSION, Optional.empty());
    final WindowInfo window2 = WindowInfo.of(HOPPING, Optional.of(Duration.ofMillis(1000)));

    new EqualsTester()
        .addEqualityGroup(
            KeyFormat.nonWindowed(format1),
            KeyFormat.nonWindowed(format1)
        )
        .addEqualityGroup(
            KeyFormat.nonWindowed(format2)
        )
        .addEqualityGroup(
            KeyFormat.windowed(format1, window1),
            KeyFormat.windowed(format1, window1)
        )
        .addEqualityGroup(
            KeyFormat.windowed(format2, window1)
        )
        .addEqualityGroup(
            KeyFormat.windowed(format1, window2)
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToString() {
    // Given:
    final FormatInfo formatInfo = FormatInfo.of(AVRO, Optional.of("something"), Optional.empty());
    final WindowInfo windowInfo = WindowInfo.of(HOPPING, Optional.of(Duration.ofMillis(10101)));

    final KeyFormat keyFormat = KeyFormat.windowed(formatInfo, windowInfo);

    // When:
    final String result = keyFormat.toString();

    // Then:
    assertThat(result, containsString(formatInfo.toString()));
    assertThat(result, containsString(windowInfo.toString()));
  }

  @Test
  public void shouldGetFormat() {
    // Given:
    final FormatInfo format = FormatInfo.of(DELIMITED, Optional.empty(), Optional.empty());
    final KeyFormat keyFormat = KeyFormat.nonWindowed(format);

    // When:
    final Format result = keyFormat.getFormat();

    // Then:
    assertThat(result, is(format.getFormat()));
  }

  @Test
  public void shouldGetFormatInfo() {
    // Given:
    final FormatInfo format = FormatInfo.of(AVRO, Optional.of("something"), Optional.empty());
    final KeyFormat keyFormat = KeyFormat.nonWindowed(format);

    // When:
    final FormatInfo result = keyFormat.getFormatInfo();

    // Then:
    assertThat(result, is(format));
  }

  @Test
  public void shouldHandleNoneWindowedFunctionsForNonWindowed() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(JSON, Optional.empty(),
        Optional.empty()));

    // Then:
    assertThat(keyFormat.isWindowed(), is(false));
    assertThat(keyFormat.getWindowType(), is(Optional.empty()));
    assertThat(keyFormat.getWindowSize(), is(Optional.empty()));
  }

  @Test
  public void shouldHandleWindowedFunctionsForWindowed() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.windowed(
        FormatInfo.of(JSON),
        WindowInfo.of(HOPPING, Optional.of(Duration.ofMinutes(4)))
    );

    // Then:
    assertThat(keyFormat.isWindowed(), is(true));
    assertThat(keyFormat.getWindowType(), is(Optional.of(HOPPING)));
    assertThat(keyFormat.getWindowSize(), is(Optional.of(Duration.ofMinutes(4))));
  }

  @Test
  public void shouldHandleWindowedWithAvroSchemaName() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.windowed(
        FormatInfo.of(AVRO, Optional.of("something"), Optional.empty()),
        WindowInfo.of(HOPPING, Optional.of(Duration.ofMinutes(4)))
    );

    // Then:
    assertThat(keyFormat.getFormatInfo(), is(FormatInfo.of(AVRO, Optional.of("something"),
        Optional.empty())));
  }

  @Test
  public void shouldHandleWindowedWithOutSize() {
    // Given:
    final KeyFormat keyFormat = KeyFormat.windowed(
        FormatInfo.of(DELIMITED),
        WindowInfo.of(SESSION, Optional.empty())
    );

    // Then:
    assertThat(keyFormat.getWindowSize(), is(Optional.empty()));
  }
}