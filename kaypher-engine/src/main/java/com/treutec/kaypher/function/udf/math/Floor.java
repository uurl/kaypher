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
import java.math.BigDecimal;

@UdfDescription(name = "Floor", description = Floor.DESCRIPTION)
public class Floor {

  static final String DESCRIPTION = "Returns the largest integer less than or equal to the "
      + "specified numeric expression. NOTE: for backwards compatibility, this returns a DOUBLE "
      + "that has a mantissa of zero.";


  @Udf
  public Double floor(@UdfParameter final Integer val) {
    return (val == null) ? null : Math.floor(val);
  }

  @Udf
  public Double floor(@UdfParameter final Long val) {
    return (val == null) ? null : Math.floor(val);
  }

  @Udf
  public Double floor(@UdfParameter final Double val) {
    return (val == null) ? null : Math.floor(val);
  }

  @Udf
  public Double floor(@UdfParameter final BigDecimal val) {
    return (val == null) ? null : Math.floor(val.doubleValue());
  }

}
