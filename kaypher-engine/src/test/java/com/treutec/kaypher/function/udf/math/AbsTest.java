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
package com.treutec.kaypher.function.udf.math;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import org.junit.Before;
import org.junit.Test;

public class AbsTest {

  private Abs udf;

  @Before
  public void setUp() {
    udf = new Abs();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.abs((Integer) null), is(nullValue()));
    assertThat(udf.abs((Long)null), is(nullValue()));
    assertThat(udf.abs((Double)null), is(nullValue()));
    assertThat(udf.abs((BigDecimal) null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.abs(-1), is(1.0));
    assertThat(udf.abs(-1L), is(1.0));
    assertThat(udf.abs(-1.0), is(1.0));
    assertThat(udf.abs(new BigDecimal(-1)), is(new BigDecimal(-1).abs()));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.abs(1), is(1.0));
    assertThat(udf.abs(1L), is(1.0));
    assertThat(udf.abs(1.0), is(1.0));
    assertThat(udf.abs(new BigDecimal(1)), is(new BigDecimal(1).abs()));
  }
}