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

import com.treutec.kaypher.function.KaypherFunctionException;
import com.treutec.kaypher.function.udf.Kudf;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public class ConcatKudf implements Kudf {
  public static final String NAME = "CONCAT";

  @Override
  public String evaluate(final Object... args) {
    if (args.length < 2) {
      throw new KaypherFunctionException(NAME + " should have at least two input argument.");
    }

    return Arrays.stream(args)
        .filter(Objects::nonNull)
        .map(Object::toString)
        .collect(Collectors.joining());
  }
}
