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
import com.treutec.kaypher.util.KaypherConstants;

@UdfDescription(name = "mask", author = KaypherConstants.CONFLUENT_AUTHOR,
    description = "Returns a version of the input string with every character replaced by a mask."
        + " Default masking rules will replace all upper-case characters with 'X', all lower-case"
        + " characters with 'x', all digits with 'n', and any other character with '-'.")
public class MaskKudf {

  @Udf(description = "Returns a masked version of the input string. All characters of the input"
      + " will be replaced according to the default masking rules.")
  public String mask(final String input) {
    return doMask(new Masker(), input);
  }

  // TODO these descriptions would be much easier and more structured if we had an annotation for
  // each param (a bit like javadoc) as it's basically the extra params that are getting described
  // in each variant of the UDF
  @Udf(description = "Returns a masked version of the input string. All characters of the input"
      + " will be replaced with the specified masking characters: e.g."
      + " mask(input, upperCaseMask, lowerCaseMask, digitMask, otherMask)."
      + " Pass NULL for any of the mask characters to prevent masking of that character type.")
  public String mask(final String input, final String upper, final String lower, final String digit,
      final String other) {
    // TODO once KAYPHER gains Char sql-datatype support we should change the xxMask params to int
    // (codepoint) instead of String

    // TODO really need a way for UDFs to do one-shot init() stuff instead of repeating all this
    // literal-param manipulation and validation for every single record
    final int upperMask = Masker.getMaskCharacter(upper);
    final int lowerMask = Masker.getMaskCharacter(lower);
    final int digitMask = Masker.getMaskCharacter(digit);
    final int otherMask = Masker.getMaskCharacter(other);
    final Masker masker = new Masker(upperMask, lowerMask, digitMask, otherMask);
    return doMask(masker, input);
  }

  private String doMask(final Masker masker, final String input) {
    if (input == null) {
      return null;
    }
    final StringBuilder output = new StringBuilder(input.length());
    output.append(masker.mask(input));
    return output.toString();
  }

}
