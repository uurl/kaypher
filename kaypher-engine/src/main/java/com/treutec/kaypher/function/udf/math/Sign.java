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
    name = "sign",
    author = KaypherConstants.CONFLUENT_AUTHOR,
    description = "The sign of a value."
)
public class Sign {
  @Udf(description = "Returns the sign of an INT value, denoted by 1, 0 or -1.")
  public Integer sign(
      @UdfParameter(
          value = "value",
          description = "The value to get the sign of."
      ) final Integer value
  ) {
    return value == null
        ? null
        : Integer.signum(value);
  }

  @Udf(description = "Returns the sign of an BIGINT value, denoted by 1, 0 or -1.")
  public Integer sign(
      @UdfParameter(
          value = "value",
          description = "The value to get the sign of."
      ) final Long value
  ) {
    return value == null
        ? null
        : Long.signum(value);
  }

  @Udf(description = "Returns the sign of an DOUBLE value, denoted by 1, 0 or -1.")
  public Integer sign(
      @UdfParameter(
          value = "value",
          description = "The value to get the sign of."
      ) final Double value
  ) {
    return value == null
        ? null
        : (int) Math.signum(value);
  }
}
