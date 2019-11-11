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
import com.treutec.kaypher.function.udf.UdfSchemaProvider;
import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.util.KaypherException;
import java.math.BigDecimal;
import java.util.List;

@UdfDescription(name = "Abs", description = Abs.DESCRIPTION)
public class Abs {

  static final String DESCRIPTION = "Returns the absolute value of its argument. If the argument "
      + "is not negative, the argument is returned. If the argument is negative, the negation of "
      + "the argument is returned.";


  @Udf
  public Double abs(@UdfParameter final Integer val) {
    return (val == null) ? null : (double)Math.abs(val);
  }

  @Udf
  public Double abs(@UdfParameter final Long val) {
    return (val == null) ? null : (double)Math.abs(val);
  }

  @Udf
  public Double abs(@UdfParameter final Double val) {
    return (val == null) ? null : Math.abs(val);
  }

  @Udf(schemaProvider = "provideSchema")
  public BigDecimal abs(@UdfParameter final BigDecimal val) {
    return (val == null) ? null : val.abs();
  }

  @UdfSchemaProvider
  public SqlType provideSchema(final List<SqlType> params) {
    final SqlType s = params.get(0);
    if (s.baseType() != SqlBaseType.DECIMAL) {
      throw new KaypherException("The schema provider method for Abs expects a BigDecimal parameter"
          + "type");
    }
    return s;
  }
}