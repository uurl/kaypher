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
package com.treutec.kaypher.function.udaf.placeholder;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.treutec.kaypher.function.udaf.TableUdaf;
import org.junit.Test;

public class PlaceholderTableUdafTest {

  private final TableUdaf<Long, Long, Long> udaf = PlaceholderTableUdaf.INSTANCE;

  @Test
  public void shouldInitializeAsNull() {
    assertThat(udaf.initialize(), is(nullValue()));
  }

  @Test
  public void shouldAggregateToNull() {
    assertThat(udaf.aggregate(1L, 2L), is(nullValue()));
  }

  @Test
  public void shouldUndoToNull() {
    assertThat(udaf.undo(1L, 2L), is(nullValue()));
  }

  @Test
  public void shouldMergeToNull() {
    assertThat(udaf.merge(1L, 2L), is(nullValue()));
  }
}