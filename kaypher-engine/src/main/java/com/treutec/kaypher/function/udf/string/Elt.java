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

@UdfDescription(name = "elt", description = Elt.DESCRIPTION)
public class Elt {

  static final String DESCRIPTION = "ELT() returns the Nth element of the list of strings. Returns"
      + " NULL if N is less than 1 or greater than the number of arguments. Note that this method"
      + " is 1-indexed. This is the complement to FIELD.";

  @Udf
  public String elt(
      @UdfParameter(description = "the nth element to extract") final int n,
      @UdfParameter(description = "the strings of which to extract the nth") final String... args
  ) {
    if (n < 1 || n > args.length) {
      return null;
    }

    return args[n - 1];
  }
}
