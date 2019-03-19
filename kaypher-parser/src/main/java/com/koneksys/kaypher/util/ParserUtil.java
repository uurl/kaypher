/*
 * Copyright 2019 Koneksys
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

package com.koneksys.kaypher.util;

import com.google.common.collect.ImmutableSet;
import com.koneksys.kaypher.parser.SqlBaseLexer;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class ParserUtil {
  private ParserUtil() {
  }

  private static final Set<String> LITERALS_SET = ImmutableSet.copyOf(
      IntStream.range(0, SqlBaseLexer.VOCABULARY.getMaxTokenType())
          .mapToObj(SqlBaseLexer.VOCABULARY::getLiteralName)
          .filter(Objects::nonNull)
          // literals start and end with ' - remove them
          .map(l -> l.substring(1, l.length() - 1))
          .map(String::toUpperCase)
          .collect(Collectors.toSet())
  );

  public static String escapeIfLiteral(final String name) {
    return LITERALS_SET.contains(name.toUpperCase()) ? "`" + name + "`" : name;
  }
}
