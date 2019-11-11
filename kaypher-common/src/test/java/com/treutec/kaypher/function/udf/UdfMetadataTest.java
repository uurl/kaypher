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

package com.treutec.kaypher.function.udf;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

public class UdfMetadataTest {

  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(
            new UdfMetadata("name", "desc", "auth", "ver", "path", false),
            new UdfMetadata("name", "desc", "auth", "ver", "path", false)
        )
        .addEqualityGroup(new UdfMetadata("DIF", "desc", "auth", "ver", "path", false))
        .addEqualityGroup(new UdfMetadata("name", "DIF", "auth", "ver", "path", false))
        .addEqualityGroup(new UdfMetadata("name", "desc", "DIF", "ver", "path", false))
        .addEqualityGroup(new UdfMetadata("name", "desc", "auth", "DIF", "path", false))
        .addEqualityGroup(new UdfMetadata("name", "desc", "auth", "ver", "DIF", false))
        .addEqualityGroup(new UdfMetadata("name", "desc", "auth", "ver", "path", true))
        .testEquals();
  }
}