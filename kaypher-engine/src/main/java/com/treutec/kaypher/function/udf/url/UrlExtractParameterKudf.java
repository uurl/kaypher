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

import com.google.common.base.Splitter;
import com.treutec.kaypher.function.udf.Udf;
import com.treutec.kaypher.function.udf.UdfDescription;
import com.treutec.kaypher.function.udf.UdfParameter;
import java.net.URI;
import java.util.List;

@UdfDescription(
    name = UrlExtractParameterKudf.NAME,
    description = UrlExtractParameterKudf.DESCRIPTION)
public class UrlExtractParameterKudf {

  static final String DESCRIPTION =
      "Extracts a parameter with a specified name encoded inside an "
      + "application/x-www-form-urlencoded String.";
  static final String NAME = "url_extract_parameter";

  private static final Splitter PARAM_SPLITTER = Splitter.on('&');
  private static final Splitter KV_SPLITTER = Splitter.on('=').limit(2);

  @Udf(description = DESCRIPTION)
  public String extractParam(
      @UdfParameter(description = "a valid URL") final String input,
      @UdfParameter(description = "the parameter key") final String paramToFind) {
    final String query = UrlParser.extract(input, URI::getQuery);
    if (query == null) {
      return null;
    }

    for (final String param : PARAM_SPLITTER.split(query)) {
      final List<String> kvParam = KV_SPLITTER.splitToList(param);
      if (kvParam.size() == 1 && kvParam.get(0).equals(paramToFind)) {
        return "";
      } else if (kvParam.size() == 2 && kvParam.get(0).equals(paramToFind)) {
        return kvParam.get(1);
      }
    }
    return null;
  }
}
