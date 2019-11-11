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

import com.treutec.kaypher.function.udf.Udf;
import com.treutec.kaypher.function.udf.UdfDescription;
import com.treutec.kaypher.function.udf.UdfParameter;
import com.treutec.kaypher.util.KaypherConstants;

@SuppressWarnings("WeakerAccess") // Invoked via reflection
@UdfDescription(
    name = "sqrt",
    author = KaypherConstants.CONFLUENT_AUTHOR,
    description = "The square root of a value."
)
public class Sqrt {

  @Udf(description = "Returns the correctly rounded positive square root of a DOUBLE value")
  public Double sqrt(
      @UdfParameter(
          value = "value",
          description = "The value to get the square root of."
      ) final Integer value
  ) {
    return sqrt(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the correctly rounded positive square root of a DOUBLE value")
  public Double sqrt(
      @UdfParameter(
          value = "value",
          description = "The value to get the square root of."
      ) final Long value
  ) {
    return sqrt(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the correctly rounded positive square root of a DOUBLE value")
  public Double sqrt(
      @UdfParameter(
          value = "value",
          description = "The value to get the square root of."
      ) final Double value
  ) {
    return value == null
        ? null
        : Math.sqrt(value);
  }
}
