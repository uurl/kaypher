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

@UdfDescription(name = UrlExtractPortKudf.NAME, description = UrlExtractPortKudf.DESCRIPTION)
public class UrlExtractPortKudf {

  static final String DESCRIPTION =
      "Extracts the port from an application/x-www-form-urlencoded encoded String."
          + " If there is no port or the string is invalid, this will return null.";
  static final String NAME = "url_extract_port";

  @Udf(description = DESCRIPTION)
  public Integer extractPort(
      @UdfParameter(description = "a valid URL to extract a port from")
      final String input) {
    final Integer port = UrlParser.extract(input, URI::getPort);

    // check for LT 0 because URI::getPort returns -1 if the port
    // does not exist, but UrlParser#extract will return null if
    // the URI is invalid
    return (port == null || port < 0) ? null : port;
  }
}
