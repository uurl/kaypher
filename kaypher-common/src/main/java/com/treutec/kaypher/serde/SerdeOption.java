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

import com.google.common.collect.ImmutableSet;
import java.util.Set;

public enum SerdeOption {

  /**
   * If the value schema contains only a single field, persist it as an anonymous value.
   *
   * <p>If not set, any single field value schema will be persisted within an outer object, e.g.
   * JSON Object or Avro Record.
   */
  UNWRAP_SINGLE_VALUES;

  public static Set<SerdeOption> none() {
    return ImmutableSet.of();
  }

  public static Set<SerdeOption> of(final SerdeOption... options) {
    return ImmutableSet.copyOf(options);
  }
}
