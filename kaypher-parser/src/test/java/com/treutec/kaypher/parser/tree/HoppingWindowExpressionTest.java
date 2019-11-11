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
package com.treutec.kaypher.parser.tree;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.treutec.kaypher.execution.windows.HoppingWindowExpression;
import com.treutec.kaypher.model.WindowType;
import com.treutec.kaypher.parser.NodeLocation;
import com.treutec.kaypher.serde.WindowInfo;
import java.time.Duration;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HoppingWindowExpressionTest {

  public static final NodeLocation SOME_LOCATION = new NodeLocation(0, 0);
  public static final NodeLocation OTHER_LOCATION = new NodeLocation(1, 0);

  @Test
  public void shouldImplementHashCodeAndEqualsProperty() {
    new EqualsTester()
        .addEqualityGroup(
            // Note: At the moment location does not take part in equality testing
            new HoppingWindowExpression(10, SECONDS, 20, MINUTES),
            new HoppingWindowExpression(10, SECONDS, 20, MINUTES),
            new HoppingWindowExpression(Optional.of(SOME_LOCATION), 10, SECONDS, 20, MINUTES),
            new HoppingWindowExpression(Optional.of(OTHER_LOCATION), 10, SECONDS, 20, MINUTES)
        )
        .addEqualityGroup(
            new HoppingWindowExpression(30, SECONDS, 20, MINUTES)
        )
        .addEqualityGroup(
            new HoppingWindowExpression(10, HOURS, 20, MINUTES)
        )
        .addEqualityGroup(
            new HoppingWindowExpression(10, SECONDS, 1, MINUTES)
        )
        .addEqualityGroup(
            new HoppingWindowExpression(10, SECONDS, 20, MILLISECONDS)
        )
        .testEquals();
  }

  @Test
  public void shouldReturnWindowInfo() {
    assertThat(new HoppingWindowExpression(10, SECONDS, 20, MINUTES).getWindowInfo(),
        is(WindowInfo.of(WindowType.HOPPING, Optional.of(Duration.ofSeconds(10)))));
  }
}