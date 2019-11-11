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

@UdfDescription(
    name = "ln",
    author = KaypherConstants.CONFLUENT_AUTHOR,
    description = "The natural logarithm of a value."
)
public class Ln {

  @Udf(description = "Returns the natural logarithm (base e) of an INT value.")
  public Double ln(
        @UdfParameter(
            value = "value",
            description = "the value get the natual logarithm of."
        ) final Integer value
  ) {
    return ln(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the natural logarithm (base e) of a BIGINT value.")
  public Double ln(
      @UdfParameter(
          value = "value",
          description = "the value get the natual logarithm of."
      ) final Long value
  ) {
    return ln(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the natural logarithm (base e) of a DOUBLE value.")
  public Double ln(
      @UdfParameter(
          value = "value",
          description = "the value get the natual logarithm of."
      ) final Double value
  ) {
    return value == null
        ? null
        : Math.log(value);
  }
}
