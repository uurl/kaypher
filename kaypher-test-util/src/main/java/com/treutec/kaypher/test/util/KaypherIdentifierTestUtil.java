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

package com.treutec.kaypher.test.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test utility class for ensuring unique identifier names in KAYPHER statements.
 *
 * <p>e.g. Kaypher stream and table names.
 */
public final class KaypherIdentifierTestUtil {

  private static final AtomicInteger COUNTER = new AtomicInteger();

  private KaypherIdentifierTestUtil() {
  }

  /**
   * Use this method to create a unique name for streams or topics in KAYPHER statements.
   *
   * <p>This stops one tests from interfering with each other - a common gotcha!
   *
   * @param prefix a prefix for the name, (to help more easily identify it).
   * @return a unique name that matches KAYPHER's allowed format for identifiers, starting with {@code
   * prefix}.
   */
  public static String uniqueIdentifierName(final String prefix) {
    return prefix + '_' + uniqueIdentifierName();
  }

  /**
   * Use this method to create a unique name for streams or topics in KAYPHER statements.
   *
   * <p>This stops one tests from interfering with each other - a common gotcha!
   *
   * @return a unique name that matches KAYPHER's allowed format for identifiers.
   */
  public static String uniqueIdentifierName() {
    return "ID_" + COUNTER.getAndIncrement();
  }
}
