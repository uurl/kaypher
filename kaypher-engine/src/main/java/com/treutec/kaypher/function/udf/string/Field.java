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

import com.treutec.kaypher.function.udf.Udf;
import com.treutec.kaypher.function.udf.UdfDescription;
import com.treutec.kaypher.function.udf.UdfParameter;

@UdfDescription(name = "field", description = Field.DESCRIPTION)
public class Field {

  static final String DESCRIPTION = "Returns the position (1-indexed) of str in args, or 0 if"
      + " not found. If str is NULL, the return value is 0 because NULL is not considered"
      + " equal to any value. This is the compliment to ELT.";

  @Udf
  public int field(
      @UdfParameter final String str,
      @UdfParameter final String... args
  ) {
    if (str == null) {
      return 0;
    }

    for (int i = 0; i < args.length; i++) {
      if (str.equals(args[i])) {
        return i + 1;
      }
    }

    return 0;
  }

}
