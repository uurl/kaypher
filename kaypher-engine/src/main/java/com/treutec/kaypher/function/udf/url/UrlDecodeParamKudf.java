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
package com.treutec.kaypher.function.udf.url;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.treutec.kaypher.function.KaypherFunctionException;
import com.treutec.kaypher.function.udf.Udf;
import com.treutec.kaypher.function.udf.UdfDescription;
import com.treutec.kaypher.function.udf.UdfParameter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;


@UdfDescription(name = UrlDecodeParamKudf.NAME, description = UrlDecodeParamKudf.DESCRIPTION)
public class UrlDecodeParamKudf {

  static final String DESCRIPTION =
      "Decodes a previously encoded application/x-www-form-urlencoded String";
  static final String NAME = "url_decode_param";

  @Udf(description = DESCRIPTION)
  public String decodeParam(
      @UdfParameter(description = "the value to decode") final String input) {
    try {
      return URLDecoder.decode(input, UTF_8.name());
    } catch (final UnsupportedEncodingException e) {
      throw new KaypherFunctionException(
          "url_decode udf encountered an encoding exception while decoding: " + input, e);
    }
  }
}
