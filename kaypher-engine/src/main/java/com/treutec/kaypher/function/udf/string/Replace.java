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
import com.treutec.kaypher.util.KaypherConstants;

@SuppressWarnings("unused") // Invoked via reflection.
@UdfDescription(name = "replace",
    author = KaypherConstants.CONFLUENT_AUTHOR,
    description = "Replaces all occurances of a substring in a string with a new substring.")
public class Replace {
  @Udf(description = "Returns a new string with all occurences of oldStr in str with newStr")
  public String replace(
      @UdfParameter(
          description = "The source string. If null, then function returns null.") final String str,
      @UdfParameter(
          description = "The substring to replace."
              + " If null, then function returns null.") final String oldStr,
      @UdfParameter(
          description = "The string to replace the old substrings with."
              + " If null, then function returns null.") final String newStr) {
    if (str == null || oldStr == null || newStr == null) {
      return null;
    }

    return str.replace(oldStr, newStr);
  }
}
