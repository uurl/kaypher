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
    name = "exp",
    author = KaypherConstants.CONFLUENT_AUTHOR,
    description = "The exponential of a value."
)
public class Exp {

  @Udf(description = "Returns Euler's number e raised to the power of an INT value.")
  public Double exp(
      @UdfParameter(
          value = "exponent",
          description = "the exponent to raise e to."
      ) final Integer exponent
  ) {
    return exp(exponent == null ? null : exponent.doubleValue());
  }

  @Udf(description = "Returns Euler's number e raised to the power of a BIGINT value.")
  public Double exp(
      @UdfParameter(
          value = "exponent",
          description = "the exponent to raise e to."
      ) final Long exponent
  ) {
    return exp(exponent == null ? null : exponent.doubleValue());
  }

  @Udf(description = "Returns Euler's number e raised to the power of a DOUBLE value.")
  public Double exp(
      @UdfParameter(
          value = "exponent",
          description = "the exponent to raise e to."
      ) final Double exponent
  ) {
    return exponent == null
        ? null
        : Math.exp(exponent);
  }
}