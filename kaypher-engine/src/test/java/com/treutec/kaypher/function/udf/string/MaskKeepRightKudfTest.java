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
package com.treutec.kaypher.function.udf.string;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import com.treutec.kaypher.function.KaypherFunctionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class MaskKeepRightKudfTest {
  private final MaskKeepRightKudf udf = new MaskKeepRightKudf();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldNotMaskLastNChars() {
    final String result = udf.mask("AbCd#$123xy Z", 5);
    assertThat(result, is("XxXx--nn3xy Z"));
  }

  @Test
  public void shouldNotMaskAnyCharsIfLengthTooLong() {
    final String result = udf.mask("AbCd#$123xy Z", 999);
    assertThat(result, is("AbCd#$123xy Z"));
 }

  @Test
  public void shouldThrowIfLengthIsNegative() {
    expectedException.expect(KaypherFunctionException.class);
    expectedException.expectMessage("function mask_keep_right requires a non-negative number");
    udf.mask("AbCd#$123xy Z", -1);
 }

  @Test
  public void shouldReturnNullForNullInput() {
    final String result = udf.mask(null, 5);
    assertThat(result, is(nullValue()));
 }

  @Test
  public void shouldApplyAllExplicitTypeMasks() {
    final String result = udf.mask("AbCd#$123xy Z", 5, "Q", "q", "9", "@");
    assertThat(result, is("QqQq@@993xy Z"));
  }

  @Test
  public void shouldMaskNothingIfNullMasks() {
    final String result = udf.mask("AbCd#$123xy Z", 5, null, null, null, null);
    assertThat(result, is("AbCd#$123xy Z"));
  }

  @Test
  public void shouldMaskOnlySpecifiedCharTypes() {
    final String result = udf.mask("AbCd#$123xy Z", 5, null, "q", null, "=");
    assertThat(result, is("AqCq==123xy Z"));
  }
}
