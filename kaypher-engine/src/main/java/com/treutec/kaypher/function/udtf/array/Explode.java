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
package com.treutec.kaypher.function.udtf.array;

import com.treutec.kaypher.function.udf.UdfSchemaProvider;
import com.treutec.kaypher.function.udtf.Udtf;
import com.treutec.kaypher.function.udtf.UdtfDescription;
import com.treutec.kaypher.schema.kaypher.types.SqlArray;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.util.KaypherConstants;
import com.treutec.kaypher.util.KaypherException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of the 'explode' table function. This table function takes an array of values and
 * explodes it into zero or more rows, one for each value in the array.
 */
@UdtfDescription(name = "explode", author = KaypherConstants.CONFLUENT_AUTHOR,
    description =
        "Explodes an array. This function outputs one value for each element of the array.")
public class Explode {

  @Udtf
  public <T> List<T> explode(final List<T> list) {
    return list == null ? Collections.emptyList() : list;
  }

  @Udtf(schemaProvider = "provideSchema")
  public List<BigDecimal> explodeBigDecimal(final List<BigDecimal> input) {
    return explode(input);
  }

  @UdfSchemaProvider
  public SqlType provideSchema(final List<SqlType> params) {
    final SqlType argType = params.get(0);
    if (!(argType instanceof SqlArray)) {
      throw new KaypherException("explode should be provided with an ARRAY");
    }
    return ((SqlArray) argType).getItemType();
  }
}
