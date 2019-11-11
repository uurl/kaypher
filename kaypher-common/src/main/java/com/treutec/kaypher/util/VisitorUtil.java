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

package com.treutec.kaypher.util;

public final class VisitorUtil {
  private VisitorUtil() {
  }

  public static RuntimeException unsupportedOperation(
      final Object visitor,
      final Object obj) {
    return new UnsupportedOperationException(
        String.format(
            "not yet implemented: %s.visit%s",
            visitor.getClass().getName(),
            obj.getClass().getSimpleName()
        )
    );
  }

  public static RuntimeException illegalState(
      final Object visitor,
      final Object obj) {
    return new IllegalStateException(
        String.format(
            "Type %s should never be visited by %s",
            obj.getClass(),
            visitor.getClass()));
  }
}
