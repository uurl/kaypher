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

package com.treutec.kaypher.function.udf.list;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import org.junit.Test;

public class AsArrayTest {

  @Test
  public void shouldCreateArrayFromEmpty() {
    // When:
    final List<String> array = new AsArray().asArray();

    // Then:
    assertThat(array, empty());
  }

  @Test
  public void shouldCreateSingleNullArray() {
    // When:
    final List<String> array = new AsArray().asArray((String) null);

    // Then:
    assertThat(array, is(Lists.newArrayList((String) null)));
  }

  @Test
  public void shouldCreateSingleElementArray() {
    // When:
    final List<String> array = new AsArray().asArray("foo");

    // Then:
    assertThat(array, is(ImmutableList.of("foo")));
  }

  @Test
  public void shouldCreateMultiElementArray() {
    // When:
    final List<String> array = new AsArray().asArray("foo", "bar");

    // Then:
    assertThat(array, is(ImmutableList.of("foo", "bar")));
  }

  @Test
  public void shouldCreateMultiElementArrayWithNulls() {
    // When:
    final List<String> array = new AsArray().asArray("foo", null);

    // Then:
    assertThat(array, is(Lists.newArrayList("foo", null)));
  }

  @Test
  public void shouldCreateMultiElementArrayOfInts() {
    // When:
    final List<Integer> array = new AsArray().asArray(1, 2);

    // Then:
    assertThat(array, is(ImmutableList.of(1, 2)));
  }

}