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

import com.treutec.kaypher.function.udf.Udf;
import com.treutec.kaypher.function.udf.UdfDescription;
import com.treutec.kaypher.function.udf.UdfParameter;
import java.net.URI;

@UdfDescription(name = UrlExtractHostKudf.NAME, description = UrlExtractHostKudf.DESCRIPTION)
public class UrlExtractHostKudf {

  static final String DESCRIPTION =
      "Extracts the Host Name of an application/x-www-form-urlencoded String input";
  static final String NAME = "url_extract_host";

  @Udf(description = DESCRIPTION)
  public String extractHost(
      @UdfParameter(description = "a valid URL") final String input) {
    return UrlParser.extract(input, URI::getHost);
  }
}
