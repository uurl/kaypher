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

package com.treutec.kaypher.function;

import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.function.udf.Kudf;
import com.treutec.kaypher.name.FunctionName;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;

/**
 * A wrapper around the actual table function which provides methods to get return type and
 * description, and allows the function to be invoked.
 */
@Immutable
public class KaypherTableFunction extends KaypherFunction {

  private final Kudf udtf;

  public KaypherTableFunction(
      final Function<List<Schema>, Schema> returnSchemaProvider,
      final FunctionName functionName,
      final Schema outputType,
      final List<Schema> arguments,
      final String description,
      final Kudf udtf
  ) {
    super(returnSchemaProvider, outputType, arguments, functionName, description,
        "", false
    );
    this.udtf = Objects.requireNonNull(udtf, "udtf");
  }

  public List<?> apply(final Object... args) {
    return (List<?>) udtf.evaluate(args);
  }
}
