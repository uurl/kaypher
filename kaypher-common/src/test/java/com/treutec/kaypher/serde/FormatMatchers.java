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

import com.treutec.kaypher.model.WindowType;
import java.time.Duration;
import java.util.Optional;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

public final class FormatMatchers {

  private FormatMatchers() {
  }

  public static final class KeyFormatMatchers {

    private KeyFormatMatchers() {
    }

    public static Matcher<KeyFormat> hasFormat(
        final Matcher<Format> matcher
    ) {
      return new FeatureMatcher<KeyFormat, Format>
          (matcher, "key format of", "key format") {
        @Override
        protected Format featureValueOf(final KeyFormat actual) {
          return actual.getFormat();
        }
      };
    }

    public static Matcher<KeyFormat> hasWindowType(
        final Matcher<Optional<WindowType>> matcher
    ) {
      return new FeatureMatcher<KeyFormat, Optional<WindowType>>
          (matcher, "key window type", "window type") {
        @Override
        protected Optional<WindowType> featureValueOf(final KeyFormat actual) {
          return actual.getWindowType();
        }
      };
    }

    public static Matcher<KeyFormat> hasWindowSize(
        final Matcher<Optional<Duration>> matcher
    ) {
      return new FeatureMatcher<KeyFormat, Optional<Duration>>
          (matcher, "key window size", "window size") {
        @Override
        protected Optional<Duration> featureValueOf(final KeyFormat actual) {
          return actual.getWindowSize();
        }
      };
    }
  }

}
