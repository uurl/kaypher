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
package com.treutec.kaypher.function.udaf.window;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.treutec.kaypher.function.udaf.UdafDescription;
import com.treutec.kaypher.function.udaf.placeholder.PlaceholderTableUdaf;
import org.junit.Test;

public class WindowEndKudafTest {

  @Test
  public void shouldReturnCorrectFunctionName() {
    assertThat(WindowEndKudaf.getFunctionName(),
        is(WindowEndKudaf.class.getAnnotation(UdafDescription.class).name()));
  }

  @Test
  public void shouldCreatePlaceholderTableUdaf() {
    assertThat(WindowEndKudaf.createWindowEnd(), is(instanceOf(PlaceholderTableUdaf.class)));
  }
}